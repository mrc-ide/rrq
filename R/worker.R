##' A rrq queue worker.  These are not typically for interacting with
##' but will sit and poll a queue for jobs.
##'
##' @title rrq queue worker
##' @export
rrq_worker <- R6::R6Class(
  "rrq_worker",

  public = list(
    ##' @field name The name of the worker
    name = NULL,

    ##' @field envir The environment that the worker uses for calculations
    envir = NULL,

    ##' @field keys Internally used keys
    keys = NULL,

    ##' @field queue The queue(s) that the worker is listening on
    queue = NULL,

    ##' @field con Redis connection
    con = NULL,

    verbose = NULL,

    active_task = NULL,

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
    ##' @param worker_name Optional worker name.  If omitted, a random
    ##'   name will be created.
    ##'
    ##' @param time_poll Poll time.  Longer values here will reduce the
    ##'   impact on the database but make workers less responsive to being
    ##'   killed with an interrupt (control-C or Escape).  The default
    ##'   should be good for most uses, but shorter values are used for
    ##'   debugging.
    ##'
    ##' @param timeout Optional timeout to set for the worker.  This is
    ##'   (roughly) equivalent to issuing a \code{TIMEOUT_SET} message
    ##'   after initialising the worker, except that it's guaranteed to be
    ##'   run by all workers.
    ##'
    ##' @param heartbeat_period Optional period for the heartbeat.  If
    ##'   non-NULL then a heartbeat process will be started (using
    ##'   [`rrq::heartbeat`]) which can be used to build fault
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
    ##' @param register Logical, indicating if the worker should be
    ##'   registered with the controller. Typically this is `TRUE`, but
    ##'   set to `FALSE` if you need to impersonate a worker, or create a
    ##'   temporary worker. This is passed as `FALSE` when running a task
    ##'   in a separate process.
    initialize = function(queue_id, con = redux::hiredis(),
                          key_alive = NULL, worker_name = NULL,
                          queue = NULL, time_poll = NULL, timeout = NULL,
                          heartbeat_period = NULL, verbose = TRUE,
                          register = TRUE) {
      assert_is(con, "redis_api")

      self$con <- con
      self$name <- worker_name %||% ids::adjective_animal()
      self$keys <- rrq_keys(queue_id, self$name)
      self$verbose <- verbose

      rrq_migrate_check(self$con, self$keys, TRUE)

      queue <- worker_queue(queue)
      self$queue <- rrq_key_queue(queue_id, queue)
      self$log("QUEUE", queue)

      if (self$con$SISMEMBER(self$keys$worker_name, self$name) == 1L) {
        stop("Looks like this worker exists already...")
      }

      private$store <- rrq_object_store(self$con, self$keys)
      private$time_poll <- time_poll %||% 60

      self$load_envir()
      if (register) {
        withCallingHandlers(
          worker_initialise(self, private,
                            key_alive, timeout, heartbeat_period),
          error = worker_catch_error(self, private))
      }
    },

    ##' @description Return information about this worker, a list of
    ##'   key-value pairs.
    info = function() {
      worker_info_collect(self, private)
    },

    ##' @description Create a log entry. This will print a human readable
    ##'   format to screen and a parseable format to the redis database.
    ##'
    ##' @param label Scalar character, the title of the log entry
    ##'
    ##' @param value Character vector (or null) with log values
    log = function(label, value = NULL) {
      worker_log(self$con, self$keys, label, value, self$verbose)
    },

    ##' @description Load the worker environment by creating a new
    ##'   environment object and running the create hook (if configured).
    ##'   See `$envir` on [`rrq::rrq_controller`] for details.
    load_envir = function() {
      self$log("ENVIR", "new")
      self$envir <- new.env(parent = .GlobalEnv)
      create <- self$con$GET(self$keys$envir)
      if (!is.null(create)) {
        self$log("ENVIR", "create")
        bin_to_object(create)(self$envir)
      }
    },

    ##' @description Poll for work
    ##'
    ##' @param immediate Logical, indicating if we should *not*
    ##'   do a blocking wait on the queue but instead reducing the timeout to
    ##'   zero. Intended primarily for use in the tests.
    poll = function(immediate = FALSE) {
      keys <- self$keys
      if (private$paused) {
        keys <- keys$worker_message
      } else {
        keys <- c(keys$worker_message, self$queue)
      }
      blpop(self$con, keys, private$time_poll, immediate)
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
    ##'  w orker event loop, which will continue until exiting (via a timeout
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
      if (is.null(private$timeout)) {
        private$timer <- NULL
      } else {
        private$timer <- time_checker(private$timeout)
      }
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
      self$con$SREM(self$keys$worker_name,   self$name)
      self$con$HSET(self$keys$worker_status, self$name, WORKER_EXITED)
      self$log("STOP", status)
    }
  ),

  private = list(
    paused = FALSE,
    loop_continue = FALSE,
    timer = NULL,
    timeout = NULL,
    ## Constants
    heartbeat = NULL,
    store = NULL,
    time_poll = NULL
  ))


worker_info_collect <- function(worker, private) {
  sys <- sessionInfo()
  redis_config <- worker$con$config()
  dat <- list(worker = worker$name,
              rrq_version = version_string(),
              platform = sys$platform,
              running = sys$running,
              hostname = hostname(),
              username = username(),
              queue = worker$queue,
              wd = getwd(),
              pid = process_id(),
              redis_host = redis_config$host,
              redis_port = redis_config$port)
  if (!is.null(private$heartbeat)) {
    dat$heartbeat_key <- worker$keys$worker_heartbeat
  }
  dat
}


worker_initialise <- function(worker, private, key_alive, timeout,
                              heartbeat_period) {
  con <- worker$con
  keys <- worker$keys

  private$heartbeat <- worker_heartbeat(con, keys, heartbeat_period,
                                        worker$verbose)

  worker$con$pipeline(
    redis$SADD(keys$worker_name,   worker$name),
    redis$HSET(keys$worker_status, worker$name, WORKER_IDLE),
    redis$HDEL(keys$worker_task,   worker$name),
    redis$DEL(keys$worker_log),
    redis$HSET(keys$worker_info,   worker$name, object_to_bin(worker$info())))

  if (!is.null(timeout)) {
    run_message_timeout_set(worker, private, timeout)
  }

  worker$log("ALIVE")

  ## This announces that we're up; things may monitor this
  ## queue, and worker_spawn does a BLPOP to
  if (!is.null(key_alive)) {
    worker$con$RPUSH(key_alive, worker$name)
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
  keys <- worker$keys

  if (!is.null(task)) {
    if (task[[1L]] == keys$worker_message) {
      run_message(worker, private, task[[2]])
    } else {
      worker_run_task(worker, private, task[[2]])
      private$timer <- NULL
    }
  }

  if (is.null(task) && !is.null(private$timeout)) {
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

  if (worker$verbose) {
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
  if (worker$verbose) {
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
  sprintf("    %s:%s %s", names(x), pad, as.character(x))
}


worker_log_format <- function(label, value) {
  t <- Sys.time()
  ts <- as.character(t)
  td <- sprintf("%.04f", t) # to nearest 1/10000 s
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


worker_log <- function(con, keys, label, value, verbose) {
  res <- worker_log_format(label, value)
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
