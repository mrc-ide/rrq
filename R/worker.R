##' Create a worker based on a named configuration. This is a wrapper
##' around directly constructing a [`rrq::rrq_worker`]. Create new
##' configurations by running `$worker_config_save` from the
##' [`rrq::rrq_controller`] object.
##'
##' @title Create a worker
##'
##' @param queue_id The name of the queue
##'
##' @param worker_config Optional name of the configuration. The
##'   default "localhost" configuration always exists.
##'
##' @param worker_id The id of the worker (if not given a random
##'   id is used)
##'
##' @param key_alive Optional key to write to, indicating that the
##'   worker is alive. This can be passed to
##'   [`rrq::rrq_expect_worker`] so that a controlling process can
##'   wait for workers to come up.
##'
##' @param con Redis configuration
##'
##' @param timeout How long to try and read the worker config for. Will
##'   attempt to read once a second and throw an error if config cannot
##'   be located after `timeout` seconds.
##'
##' @return A [`rrq::rrq_worker`] object. If you want to "run" the
##'   worker, you would want to use the `$loop()` method of this
##'   object. Other methods are for advanced use.
##'
##' @export
rrq_worker_from_config <- function(queue_id, worker_config = "localhost",
                                   worker_id = NULL, key_alive = NULL,
                                   con = redux::hiredis(), timeout = 5) {
  keys <- rrq_keys(queue_id)
  config_read <- function() {
    worker_config_read(con, keys, worker_config)
  }
  config <- wait_success("config not readable in time", timeout, config_read, 1)

  rrq_worker$new(queue_id, con,
                 key_alive = key_alive,
                 worker_id = worker_id,
                 queue = config$queue,
                 time_poll = config$time_poll,
                 timeout_idle = config$timeout_idle,
                 heartbeat_period = config$heartbeat_period,
                 verbose = config$verbose,
                 timeout_poll = config$timeout_poll,
                 timeout_die = config$timeout_die)
}


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

    ##' @description Constructor
    ##'
    ##' @param queue_id Name of the queue to connect to.  This will be the
    ##'   prefix to all the keys in the redis database
    ##'
    ##' @param con A redis connection
    ##'
    ##' @param key_alive Optional key that will be written once the
    ##'   worker is alive.
    ##'
    ##' @param worker_id Optional worker id.  If omitted, a random
    ##'   id will be created.
    ##'
    ##' @param time_poll Poll time.  Longer values here will reduce the
    ##'   impact on the database but make workers less responsive to being
    ##'   killed with an interrupt (control-C or Escape).  The default
    ##'   should be good for most uses, but shorter values are used for
    ##'   debugging.
    ##'
    ##' @param timeout_idle Optional timeout to set for the worker.  This is
    ##'   (roughly) equivalent to issuing a \code{TIMEOUT_SET} message
    ##'   after initialising the worker, except that it's guaranteed to be
    ##'   run by all workers.
    ##'
    ##' @param heartbeat_period Optional period for the heartbeat.  If
    ##'   non-NULL then a heartbeat process will be started (using
    ##'   [`rrq::rrq_heartbeat`]) which can be used to build fault
    ##'   tolerant queues.
    ##'
    ##' @param verbose Logical, indicating if the worker should print
    ##'   logging output to the screen.  Logging to screen has a small but
    ##'   measurable performance cost, and if you will not collect system
    ##'   logs from the worker then it is wasted time.  Logging to the
    ##'   redis server is always enabled.
    ##'
    ##' @param queue Queues to listen on, listed in decreasing order of
    ##'   priority. If not given, we listen on the "default" queue.
    ##'
    ##' @param timeout_poll Optional timeout indicating how long to wait
    ##'   for a background process to produce stdout or stderr. Only used
    ##'   for tasks queued with `separate_process` `TRUE`.
    ##'
    ##' @param timeout_die Optional timeout indicating how long to wait
    ##'   wait for the background process to respond to SIGTERM before
    ##'   we stop the worker. Only used for tasks queued with
    ##'   `separate_process` `TRUE`.
    ##'
    ##' @param is_child Logical, used to indicate that this is a child of
    ##'   the real worker.  If `is_child` is `TRUE`, then most other
    ##'   arguments here have no effect (e.g., `queue` all the timeout /
    ##'   idle / polling arguments) as they come from the parent.
    initialize = function(queue_id, con = redux::hiredis(),
                          key_alive = NULL, worker_id = NULL,
                          queue = NULL, time_poll = NULL, timeout_idle = NULL,
                          heartbeat_period = NULL, verbose = TRUE,
                          is_child = FALSE, timeout_poll = 1, timeout_die = 2) {
      assert_is(con, "redis_api")

      private$con <- con
      self$id <- worker_id %||% ids::adjective_animal()
      lockBinding("id", self)
      private$keys <- rrq_keys(queue_id, self$id)
      private$verbose <- verbose
      private$is_child <- is_child

      rrq_version_check(private$con, private$keys)

      worker_exists <- con$SISMEMBER(private$keys$worker_id, self$id) == 1L
      if (is_child != worker_exists) {
        if (is_child) {
          stop("Can't be a child of nonexistant worker...")
        } else {
          stop("Looks like this worker exists already...")
        }
      }

      private$store <- rrq_object_store(private$con, private$keys)
      private$time_poll <- time_poll %||% 60
      private$timeout_poll <- timeout_poll
      private$timeout_die <- timeout_die

      if (private$is_child) {
        self$log("CHILD")
        self$load_envir()
      } else {
        withCallingHandlers(
          worker_initialise(self, private, worker_queue(queue),
                            key_alive, timeout_idle, heartbeat_period),
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
      worker_log(private$con, private$keys, label, value, private$is_child,
                 private$verbose)
    },

    ##' @description Load the worker environment by creating a new
    ##'   environment object and running the create hook (if configured).
    ##'   See `$envir` on [`rrq::rrq_controller`] for details.
    load_envir = function() {
      self$log("ENVIR", "new")
      private$envir <- new.env(parent = .GlobalEnv)
      create <- private$con$GET(private$keys$envir)
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
      keys <- private$keys
      if (private$paused) {
        keys <- keys$worker_message
      } else {
        keys <- c(keys$worker_message, private$queue)
      }
      blpop(private$con, keys, private$time_poll, immediate)
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
      if (is.null(private$timeout_idle)) {
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
          stop("rrq_task_progress_update called with no active task")
        } else {
          return(invisible())
        }
      }
      private$con$HSET(private$keys$task_progress, task_id,
                       object_to_bin(value))
      invisible()
    },

    ##' @description Evaluate a task
    ##'
    ##' @param task_id A task identifier. It is undefined what happens if
    ##'   this identifier does not exist.
    task_eval = function(task_id) {
      cache$active_worker <- self
      on.exit(cache$active_worker <- NULL)
      task <- bin_to_object(private$con$HGET(private$keys$task_expr, task_id))
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
      private$con$SREM(private$keys$worker_id,     self$id)
      private$con$HSET(private$keys$worker_status, self$id, WORKER_EXITED)
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
    con = NULL,
    heartbeat = NULL,
    keys = NULL,
    queue = NULL,
    store = NULL,
    time_poll = NULL,
    verbose = NULL,
    is_child = NULL,
    timeout_poll = NULL,
    timeout_die = NULL
  ))


worker_info_collect <- function(worker, private) {
  sys <- sessionInfo()
  redis_config <- private$con$config()
  dat <- list(worker = worker$id,
              rrq_version = version_info(),
              platform = sys$platform,
              running = sys$running,
              hostname = hostname(),
              username = username(),
              queue = private$queue,
              wd = getwd(),
              pid = process_id(),
              redis_host = redis_config$host,
              redis_port = redis_config$port)
  if (!is.null(private$heartbeat)) {
    dat$heartbeat_key <- private$keys$worker_heartbeat
  }
  dat
}


worker_initialise <- function(worker, private, queue, key_alive, timeout_idle,
                              heartbeat_period) {
  con <- private$con
  keys <- private$keys

  private$heartbeat <- worker_heartbeat(con, keys, heartbeat_period,
                                        private$verbose)
  private$queue <- rrq_key_queue(keys$queue_id, queue)

  con$pipeline(
    redis$SADD(keys$worker_id,     worker$id),
    redis$HSET(keys$worker_status, worker$id, WORKER_IDLE),
    redis$HDEL(keys$worker_task,   worker$id),
    redis$DEL(keys$worker_log),
    redis$HSET(keys$worker_info,   worker$id, object_to_bin(worker$info())))

  if (!is.null(timeout_idle)) {
    run_message_timeout_set(worker, private, timeout_idle)
  }

  worker$log("ALIVE")
  worker$load_envir()
  worker$log("QUEUE", queue)

  ## This announces that we're up; things may monitor this
  ## queue, and rrq_worker_spawn does a BLPOP to
  if (!is.null(key_alive)) {
    con$RPUSH(key_alive, worker$id)
  }
}


## Controlled stop:
rrq_worker_stop <- function(worker, message) {
  structure(list(worker = worker, message = message),
            class = c("rrq_worker_stop", "error", "condition"))
}


## One step of a worker life cycle; i.e., the least we can
## interestingly do
worker_step <- function(worker, private, immediate) {
  task <- worker$poll(immediate)
  keys <- private$keys

  if (!is.null(task)) {
    if (task[[1L]] == keys$worker_message) {
      run_message(worker, private, task[[2]])
    } else {
      worker_run_task(worker, private, task[[2]])
      private$timer <- NULL
    }
  }

  if (is.null(task) && !is.null(private$timeout_idle)) {
    if (is.null(private$timer)) {
      worker$timer_start()
    }
    if (private$timer() <= 0L) {
      stop(rrq_worker_stop(worker, "TIMEOUT"))
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


worker_log <- function(con, keys, label, value, is_child, verbose) {
  res <- worker_log_format(label, value, is_child)
  if (verbose) {
    message(res$screen)
  }
  con$RPUSH(keys$worker_log, res$redis)
}


worker_queue <- function(queue) {
  if (is.null(queue)) {
    queue <- QUEUE_DEFAULT
  } else {
    assert_character(queue)
    if (!(QUEUE_DEFAULT %in% queue)) {
      queue <- c(queue, QUEUE_DEFAULT)
    }
  }
  queue
}
