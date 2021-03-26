##' A rrq queue worker.  These are not for interacting with but will
##' sit and poll a queue for jobs.
##' @title rrq queue worker
##' @inheritParams rrq_controller
##'
##' @param queue_id Name of the queue to connect to.  This will be the
##'   prefix to all the keys in the redis database
##'
##' @param key_alive Optional key that will be written once the
##'   worker is alive.
##'
##' @param worker_name Optional worker name.  If omitted, a random
##'   name will be created.
##'
##' @param time_poll Poll time.  Longer values here will reduce the
##'   impact on the database but make workers less responsive to being
##'   killed with an interrupt.  The default should be good for most
##'   uses, but shorter values are used for debugging.
##'
##' @param timeout Optional timeout to set for the worker.  This is
##'   (roughly) equivalent to issuing a \code{TIMEOUT_SET} message
##'   after initialising the worker, except that it's guaranteed to be
##'   run by all workers.
##'
##' @param heartbeat_period Optional period for the heartbeat.  If
##'   non-NULL then a heartbeat process will be started (using the
##'   \code{heartbeatr} package) which can be used to build fault
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
##' @export
rrq_worker <- function(queue_id, con = redux::hiredis(), key_alive = NULL,
                       worker_name = NULL, queue = NULL,
                       time_poll = NULL, timeout = NULL,
                       heartbeat_period = NULL, verbose = TRUE) {
  w <- rrq_worker_$new(con, queue_id, key_alive, worker_name, queue,
                       time_poll, timeout, heartbeat_period, verbose)
  w$loop()
  invisible()
}


rrq_worker_ <- R6::R6Class(
  "rrq_worker",

  public = list(
    name = NULL,
    envir = NULL,
    keys = NULL,
    queue = NULL,
    deferred_set = NULL,
    con = NULL,
    db = NULL,
    paused = FALSE,
    time_poll = NULL,
    timeout = NULL,
    timer = NULL,
    heartbeat = NULL,
    loop_continue = NULL,
    verbose = NULL,

    loop_task = NULL,
    active_task = NULL,

    initialize = function(con, queue_id, key_alive = NULL, worker_name = NULL,
                          queue = NULL, time_poll = NULL, timeout = NULL,
                          heartbeat_period = NULL, verbose = TRUE,
                          register = TRUE) {
      assert_is(con, "redis_api")

      self$con <- con
      self$name <- worker_name %||% ids::adjective_animal()
      self$keys <- rrq_keys(queue_id, self$name)
      self$verbose <- verbose

      queue <- worker_queue(queue)
      self$queue <- rrq_key_queue(queue_id, queue)
      self$deferred_set <- self$keys$deferred_set
      self$log("QUEUE", queue)

      if (self$con$SISMEMBER(self$keys$worker_name, self$name) == 1L) {
        stop("Looks like this worker exists already...")
      }

      self$db <- rrq_db(self$con, self$keys)
      self$time_poll <- time_poll %||% 60

      self$load_envir()
      if (register) {
        withCallingHandlers(
          worker_initialise(self, key_alive, timeout, heartbeat_period),
          error = worker_catch_error(self))
      }
    },

    info = function() {
      worker_info_collect(self)
    },

    log = function(label, value = NULL) {
      worker_log(self$con, self$keys, label, value, self$verbose)
    },

    load_envir = function() {
      self$log("ENVIR", "new")
      self$envir <- new.env(parent = .GlobalEnv)
      create <- self$con$GET(self$keys$envir)
      if (!is.null(create)) {
        self$log("ENVIR", "create")
        bin_to_object(create)(self$envir)
      }
    },

    poll = function(immediate = FALSE) {
      keys <- self$keys
      if (self$paused) {
        keys <- keys$worker_message
      } else {
        keys <- c(keys$worker_message, self$queue)
      }
      self$loop_task <- blpop(self$con, keys, self$time_poll, immediate)
    },

    step = function(immediate = FALSE) {
      worker_step(self, immediate)
    },

    loop = function(immediate = FALSE) {
      worker_loop(self, immediate)
    },

    run_task = function(task_id) {
      worker_run_task(self, task_id)
    },

    run_message = function(msg) {
      run_message(self, msg)
    },

    format = function() {
      worker_format(self)
    },

    timer_start = function() {
      if (is.null(self$timeout)) {
        self$timer <- NULL
      } else {
        self$timer <- time_checker(self$timeout)
      }
    },

    shutdown = function(status = "OK", graceful = TRUE) {
      if (!is.null(self$heartbeat)) {
        self$log("HEARTBEAT", "stopping")
        tryCatch(
          self$heartbeat$stop(graceful),
          error = function(e) message("Could not stop heartbeat"))
      }
      self$con$SREM(self$keys$worker_name,   self$name)
      self$con$HSET(self$keys$worker_status, self$name, WORKER_EXITED)
      self$log("STOP", status)
    }
  ))


worker_info_collect <- function(worker) {
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
  if (!is.null(worker$heartbeat)) {
    dat$heartbeat_key <- worker$keys$worker_heartbeat
  }
  dat
}


worker_initialise <- function(worker, key_alive, timeout, heartbeat_period) {
  con <- worker$con
  keys <- worker$keys

  worker$heartbeat <- heartbeat(con, keys, heartbeat_period, worker$verbose)

  worker$con$pipeline(
    redis$SADD(keys$worker_name,   worker$name),
    redis$HSET(keys$worker_status, worker$name, WORKER_IDLE),
    redis$HDEL(keys$worker_task,   worker$name),
    redis$DEL(keys$worker_log),
    redis$HSET(keys$worker_info,   worker$name, object_to_bin(worker$info())))

  if (!is.null(timeout)) {
    run_message_timeout_set(worker, timeout)
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


worker_send_signal <- function(con, keys, signal, worker_ids) {
  if (length(worker_ids) > 0L) {
    for (key in rrq_key_worker_heartbeat(keys$queue_id, worker_ids)) {
      heartbeatr::heartbeat_send_signal(con, key, signal)
    }
  }
}


## One step of a worker life cycle; i.e., the least we can
## interestingly do
worker_step <- function(worker, immediate) {
  task <- worker$poll(immediate)
  keys <- worker$keys

  if (!is.null(task)) {
    if (task[[1L]] == keys$worker_message) {
      worker$run_message(task[[2L]])
    } else {
      worker$run_task(task[[2L]])
      worker$timer <- NULL
    }
  }

  if (is.null(task) && !is.null(worker$timeout)) {
    if (is.null(worker$timer)) {
      worker$timer_start()
    }
    if (worker$timer() <= 0L) {
      stop(rrq_worker_stop(worker, "TIMEOUT"))
    }
  }
}


worker_loop <- function(worker, immediate = FALSE) {
  cache$active_worker <- worker
  on.exit(cache$active_worker <- NULL)

  if (worker$verbose) {
    message(paste0(worker_banner("start"), collapse = "\n"))
    message(paste0(worker$format(), collapse = "\n"))
  }

  worker$loop_continue <- TRUE
  while (worker$loop_continue) {
    tryCatch({
      tryCatch(worker$step(immediate),
               interrupt = worker_catch_interrupt(worker))
    },
    rrq_worker_stop = worker_catch_stop(worker),
    error = worker_catch_error(worker))
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


worker_catch_stop <- function(worker) {
  force(worker)
  function(e) {
    worker$loop_continue <- FALSE
    worker$shutdown(sprintf("OK (%s)", e$message), TRUE)
  }
}


worker_catch_error <- function(worker) {
  force(worker)
  function(e) {
    worker$loop_continue <- FALSE
    worker$shutdown("ERROR", FALSE)
    message("This is an uncaught error in rrq, probably a bug!")
    stop(e)
  }
}


worker_catch_interrupt <- function(worker) {
  force(worker)

  queue_type <- set_names(
    c("message", rep("queue", length(worker$queue))),
    c(worker$keys$worker_message, worker$queue))

  function(e) {
    ## NOTE: this won't recursively catch interrupts.  Especially
    ## on a high-latency connection this might be long enough for
    ## a second interrupt to arrive.  We don't deal with that and
    ## it will be about the same as a SIGTERM - we'll just die.
    ## But that will disable the heartbeat so it should all end up
    ## OK.
    worker$log("INTERRUPT")

    active <- worker$active_task
    if (!is.null(active)) {
      worker_run_task_cleanup(worker, TASK_INTERRUPTED, NULL)
    }

    ## There are two ways that interrupt happens (ignoring the
    ## race condition where it comes in neither)
    ##
    ## 1. Interrupt a job; in that case, we have task data in 'active'
    ##    above and everything is all OK; we just mark this as done.
    ##
    ## 2. During BLPOP.  In that case we need to re-queue the
    ##    message or the job or it is lost from the queue
    ##    entirely.  This would include things like a STOP
    ##    message, or a new task to run.
    ##
    ## NOTE that a SIGTERM signal might result in lost messages.
    task <- worker$loop_task
    if (!is.null(task[[2]]) && !identical(task[[2]], active$task_id)) {
      label <- sprintf("%s:%s", queue_type[[task[[1]]]], task[[2]])
      worker$log("REQUEUE", label)
      worker$con$LPUSH(task[[1]], task[[2]])
    }
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
