##' A rrq queue worker.  These are not typically for interacting with
##' but will sit and poll a queue for jobs.
##'
##' @title rrq queue worker
##' @export
rrq_worker <- R6::R6Class(
  "rrq_worker",
  cloneable = FALSE,

  public = list(
    ##' @field id The id of the worker
    id = NULL,

    ##' @field config The name of the configuration used by this worker
    config = NULL,

    ##' @field controller An rrq controller object
    controller = NULL,

    ##' @description Constructor
    ##'
    ##' @param queue_id The queue id
    ##'
    ##' @param name_config Optional name of the configuration. The
    ##'   default "localhost" configuration always exists. Create new
    ##'   configurations using the `$worker_config_save` method on the
    ##'  [`rrq::rrq_controller`] object.
    ##'
    ##' @param worker_id Optional worker id.  If omitted, a random
    ##'   id will be created.
    ##'
    ##' @param timeout_config How long to try and read the worker
    ##'   configuration for. Will attempt to read once a second and throw
    ##'   an error if config cannot be located after `timeout` seconds.
    ##'   Use this to create workers before their configurations are
    ##'   available. The default (0) is to assume that the configuration
    ##'   is immediately available.
    ##'
    ##' @param is_child Logical, used to indicate that this is a child of
    ##'   the real worker.  If `is_child` is `TRUE`, then most other
    ##'   arguments here have no effect (e.g., `queue` all the timeout /
    ##'   idle / polling arguments) as they come from the parent.
    ##'   Not for general use.
    ##'
    ##' @param con A redis connection
    initialize = function(queue_id, name_config = "localhost",
                          worker_id = NULL, timeout_config = 0,
                          is_child = FALSE, con = redux::hiredis()) {
      assert_scalar_character(queue_id)
      assert_is(con, "redis_api")

      self$id <- worker_id %||% ids::adjective_animal()
      self$config <- name_config
      self$controller <- rrq_controller2(queue_id, con, check_version = TRUE)

      if (is_child != rrq_worker_exists(self$id, self$controller)) {
        if (is_child) {
          cli::cli_abort(
            c("Can't be a child of nonexistant worker",
              i = "Worker '{worker_id} does not exist for queue '{queue_id}'"))
        } else {
          cli::cli_abort(
            c("Looks like this worker exists already",
              i = "Worker '{worker_id} already exists for queue '{queue_id}'"))
        }
      }

      config <- rrq_worker_config_read(name_config,
                                       timeout = timeout_config,
                                       controller = self$controller)

      private$verbose <- config$verbose
      private$is_child <- is_child
      private$poll_queue <- config$poll_queue
      private$poll_process <- config$poll_process
      private$timeout_process_die <- config$timeout_process_die
      private$key_queue <- rrq_key_queue(queue_id, config$queue)
      private$key_log <- rrq_key_worker_log(queue_id, self$id)
      private$key_message <- rrq_key_worker_message(queue_id, self$id)
      private$key_response <- rrq_key_worker_response(queue_id, self$id)

      if (private$is_child) {
        self$log("CHILD")
        self$load_envir()
      } else {
        withCallingHandlers(
          worker_initialise(self, private, config),
          error = worker_catch_error(self, private))
      }
    },

    ##' @description Return information about this worker, a list of
    ##'   key-value pairs.
    info = function() {
      worker_info_collect(self, private)
    },

    ##' @description Create a log entry. This will print a human readable
    ##'   format to screen and a machine-readable format to the redis database.
    ##'
    ##' @param label Scalar character, the title of the log entry
    ##'
    ##' @param value Character vector (or null) with log values
    log = function(label, value = NULL) {
      con <- self$controller$con
      worker_log(con, private$key_log, label, value, private$is_child,
                 private$verbose)
    },

    ##' @description Load the worker environment by creating a new
    ##'   environment object and running the create hook (if configured).
    ##'   See [rrq::rrq_worker_envir_set()] for details.
    load_envir = function() {
      self$log("ENVIR", "new")
      private$envir <- new.env(parent = .GlobalEnv)
      create <- self$controller$con$GET(self$controller$keys$envir)
      if (!is.null(create)) {
        self$log("ENVIR", "create")
        bin_to_object(create)(private$envir)
      }
    },

    ##' @description Poll for work
    ##'
    ##' @param immediate Logical, indicating if we should *not*
    ##'   do a blocking wait on the queue but instead reducing the timeout to
    ##'   zero. Intended primarily for use in the tests.
    poll = function(immediate = FALSE) {
      con <- self$controller$con
      keys <- c(private$key_message,
                if (!private$paused) private$key_queue)
      blpop(con, keys, private$poll_queue, immediate)
    },

    ##' @description Take a single "step". This consists of
    ##'
    ##' 1. Poll for work (`$poll()`)
    ##' 2. If work found, run it (either a task or a message)
    ##' 3. If work not found, check the timeout
    ##'
    ##' @param immediate Logical, indicating if we should *not*
    ##'   do a blocking wait on the queue but instead reducing the timeout to
    ##'   zero. Intended primarily for use in the tests.
    step = function(immediate = FALSE) {
      worker_step(self, private, immediate)
    },

    ##' @description The main worker loop. Use this to set up the main
    ##'  worker event loop, which will continue until exiting (via a timeout
    ##'  or message).
    ##'
    ##' @param immediate Logical, indicating if we should *not*
    ##'   do a blocking wait on the queue but instead reducing the timeout to
    ##'   zero. Intended primarily for use in the tests.
    loop = function(immediate = FALSE) {
      worker_loop(self, private, immediate)
    },

    ##' @description Create a nice string representation of the worker.
    ##'   Used automatically to print the worker by R6.
    format = function() {
      worker_format(self)
    },

    ##' @description Start the timer
    timer_start = function() {
      if (private$timeout_idle == Inf) {
        private$timer <- NULL
      } else {
        private$timer <- time_checker(private$timeout_idle)
      }
    },

    ##' @description Submit a progress message. See
    ##' [rrq::rrq_task_progress_update()] for details of this mechanism.
    ##'
    ##' @param value An R object with the contents of the update. This
    ##'   will overwrite any previous progress value, and can be retrieved
    ##'   from a [rrq::rrq_controller] with the
    ##'   `$task_progress` method.  A value of `NULL` will appear
    ##'   to clear the status, as `NULL` will also be returned if no
    ##'   status is found for a task.
    ##'
    ##' @param error Logical, indicating if we should throw an error if
    ##'   not running as an `rrq` task. Set this to `FALSE` if
    ##'   you want code to work without modification within and outside of
    ##'   an `rrq` job, or to `TRUE` if you want to be sure that
    ##'   progress messages have made it to the server.
    progress = function(value, error = TRUE) {
      task_id <- private$active_task_id
      if (is.null(task_id)) {
        if (error) {
          cli::cli_abort("rrq_task_progress_update called with no active task")
        } else {
          return(invisible())
        }
      }
      con <- self$controller$con
      keys <- self$controller$keys
      con$HSET(keys$task_progress, task_id, object_to_bin(value))
      invisible()
    },

    ##' @description Evaluate a task. When running a task on a separate
    ##'   process, we will always set two environment variables:
    ##'     * `RRQ_WORKER_ID` this is the id field
    ##'     * `RRQ_TASK_ID` this is the task id
    ##'
    ##' @param task_id A task identifier. It is undefined what happens if
    ##'   this identifier does not exist.
    task_eval = function(task_id) {
      cache$active_worker <- self
      on.exit(cache$active_worker <- NULL)
      con <- self$controller$con
      keys <- self$controller$keys
      task <- bin_to_object(con$HGET(keys$task_expr, task_id))
      private$active_task_id <- task_id
      worker_run_task_local(task, self, private)
    },

    ##' @description Stop the worker
    ##'
    ##' @param status the worker status; typically be one of `OK` or `ERROR`
    ##'   but can be any string
    ##'
    ##' @param graceful Logical, indicating if we should request a
    ##'   graceful shutdown of the heartbeat, if running.
    shutdown = function(status = "OK", graceful = TRUE) {
      if (!is.null(private$heartbeat)) {
        self$log("HEARTBEAT", "stopping")
        tryCatch(
          private$heartbeat$stop(graceful),
          error = function(e) message("Could not stop heartbeat"))
      }
      con <- self$controller$con
      keys <- self$controller$keys
      con$SREM(keys$worker_id,     self$id)
      con$HSET(keys$worker_status, self$id, WORKER_EXITED)
      self$log("STOP", status)
    }
  ),

  private = list(
    active_task_id = NULL,
    envir = NULL,
    loop_continue = FALSE,
    paused = FALSE,
    timer = NULL,
    timeout_idle = NULL,
    ## Constants
    heartbeat = NULL,
    key_queue = NULL,
    key_log = NULL,
    key_message = NULL,
    key_response = NULL,
    key_heartbeat = NULL,
    poll_queue = NULL,
    verbose = NULL,
    is_child = NULL,
    poll_process = NULL,
    timeout_process_die = NULL
  ))


worker_info_collect <- function(worker, private) {
  sys <- sessionInfo()
  redis_config <- worker$controller$con$config()
  dat <- list(worker = worker$id,
              config = worker$config,
              rrq_version = version_info(),
              platform = sys$platform,
              running = sys$running,
              hostname = hostname(),
              username = username(),
              queue = private$key_queue,
              wd = getwd(),
              pid = process_id(),
              redis_host = redis_config$host,
              redis_port = redis_config$port)
  if (!is.null(private$heartbeat)) {
    dat$heartbeat_key <- private$key_heartbeat
  }
  dat
}


worker_initialise <- function(worker, private, config) {
  con <- worker$controller$con
  keys <- worker$controller$keys

  if (!is.null(config$heartbeat_period)) {
    private$key_heartbeat <- rrq_key_worker_heartbeat(keys$queue_id, worker$id)
    worker$log("HEARTBEAT", private$key_heartbeat)
    private$heartbeat <- rrq_heartbeat$new(private$key_heartbeat,
                                           config$heartbeat_period,
                                           config = con$config())
    worker$log("HEARTBEAT", "OK")
  }

  res <- con$pipeline(
    redis$SADD(keys$worker_id,     worker$id),
    redis$HSET(keys$worker_status, worker$id, WORKER_IDLE),
    redis$HDEL(keys$worker_task,   worker$id),
    redis$DEL(c(private$key_log, private$key_message, private$key_response)),
    redis$HSET(keys$worker_info,   worker$id, object_to_bin(worker$info())),
    redis$HGET(keys$worker_alive, worker$id))

  key_alive <- res[[6]]

  if (!is.null(config$timeout_idle)) {
    run_message_timeout_set(worker, private, config$timeout_idle)
  }

  worker$log("ALIVE")
  worker$load_envir()
  worker$log("QUEUE", config$queue)

  ## This announces that we're up; things may monitor this
  ## queue, and rrq_worker_spawn does a BLPOP to
  if (!is.null(key_alive)) {
    con$RPUSH(key_alive, worker$id)
  }
}


## Controlled stop:
rrq_worker_stop_condition <- function(worker, message) {
  structure(list(worker = worker, message = message),
            class = c("rrq_worker_stop", "error", "condition"))
}


## One step of a worker life cycle; i.e., the least we can
## interestingly do
worker_step <- function(worker, private, immediate) {
  task <- worker$poll(immediate)

  if (!is.null(task)) {
    if (task[[1L]] == private$key_message) {
      run_message(worker, private, task[[2]])
    } else {
      worker_run_task(worker, private, task[[2]])
      private$timer <- NULL
    }
  }

  if (is.null(task) && private$timeout_idle < Inf) {
    if (is.null(private$timer)) {
      worker$timer_start()
    }
    if (private$timer() <= 0L) {
      stop(rrq_worker_stop_condition(worker, "TIMEOUT"))
    }
  }
}


worker_loop <- function(worker, private, immediate = FALSE) {
  cache$active_worker <- worker
  on.exit(cache$active_worker <- NULL)

  if (private$verbose) {
    message(paste0(worker_banner("start"), collapse = "\n"))
    message(paste0(worker$format(), collapse = "\n"))
  }

  private$loop_continue <- TRUE
  while (private$loop_continue) {
    tryCatch(
      worker$step(immediate),
      rrq_worker_stop = worker_catch_stop(worker, private),
      error = worker_catch_error(worker, private))
  }
  if (private$verbose) {
    message(paste0(worker_banner("stop"), collapse = "\n"))
  }
}


worker_banner <- function(name) {
  path <- system.file(file.path("banner", name), package = "rrq",
                      mustWork = TRUE)
  readLines(path)
}


worker_catch_stop <- function(worker, private) {
  force(worker)
  force(private)
  function(e) {
    private$loop_continue <- FALSE
    worker$shutdown(sprintf("OK (%s)", e$message), TRUE)
  }
}


worker_catch_error <- function(worker, private) {
  force(worker)
  force(private)
  function(e) {
    private$loop_continue <- FALSE
    worker$shutdown("ERROR", FALSE)
    message("This is an uncaught error in rrq, probably a bug!")
    stop(e)
  }
}


worker_format <- function(worker) {
  x <- worker$info()
  x$heartbeat_key <- x$heartbeat_key %||% "<not set>"
  n <- nchar(names(x))
  pad <- vcapply(max(n) - n, function(n) strrep(" ", n))
  queue_indent <- paste("\n", strrep(" ", max(n)), "    ")
  x$queue <- paste(x$queue, collapse = queue_indent)
  sprintf("    %s:%s %s", names(x), pad, as.character(x))
}


worker_log_format <- function(label, value, is_child) {
  t <- Sys.time()
  ts <- as.character(t)
  td <- sprintf("%.04f", t) # to nearest 1/10000 s
  if (is_child) {
    ts <- sprintf("%s (%d)", ts, Sys.getpid())
    td <- sprintf("%s/%d", td, Sys.getpid())
  }
  if (is.null(value)) {
    str_redis <- sprintf("%s %s", td, label)
    str_screen <- sprintf("[%s] %s", ts, label)
  } else {
    str_redis <- sprintf("%s %s %s",
                         td, label, paste(value, collapse = "\n"))
    ## Try and make nicely printing logs for the case where the
    ## value length is longer than 1:
    lab <- c(label, rep_len(blank(nchar(label)), length(value) - 1L))
    str_screen <- paste(sprintf("[%s] %s %s", ts, lab, value),
                        collapse = "\n")
  }

  list(screen = str_screen, redis = str_redis)
}


worker_log <- function(con, key_log, label, value, is_child, verbose) {
  res <- worker_log_format(label, value, is_child)
  if (verbose) {
    message(res$screen)
  }
  con$RPUSH(key_log, res$redis)
}
