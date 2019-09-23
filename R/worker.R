##' A rrq queue worker.  These are not for interacting with but will
##' sit and poll a queue for jobs.
##' @title rrq queue worker
##' @inheritParams rrq_controller
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
##' @param log_path Optional log path, used for storing per-task logs,
##'   rather than leaving them within the worker logs.  This affects
##'   only the context queue and not the rrq queue, which is not
##'   logged.  This path will be intepreted as relative to the context
##'   root.
##'
##' @param timeout Optional timeout to set for the worker.  This is
##'   (roughly) quivalent to issuing a \code{TIMEOUT_SET} message
##'   after initialising the worker, except that it's guaranteed to be
##'   run by all workers.
##'
##' @param heartbeat_period Optional period for the heartbeat.  If
##'   non-NULL then a heartbeat process will be started (using the
##'   \code{heartbeatr} package) which can be used to build fault
##'   tolerant queues.
##'
##' @export
rrq_worker <- function(context, con, key_alive = NULL, worker_name = NULL,
                       time_poll = NULL, log_path = NULL, timeout = NULL,
                       heartbeat_period = NULL) {
  w <- R6_rrq_worker$new(context, con, key_alive, worker_name, time_poll,
                         log_path, timeout, heartbeat_period)
  w$loop()
  invisible()
}


R6_rrq_worker <- R6::R6Class(
  "rrq_worker",

  public = list(
    name = NULL,
    context = NULL,
    envir = NULL,
    keys = NULL,
    con = NULL,
    db = NULL,
    paused = FALSE,
    log_path = NULL,
    time_poll = NULL,
    timeout = NULL,
    timer = NULL,
    heartbeat = NULL,
    cores = NULL,
    loop_task = NULL,
    loop_continue = NULL,

    initialize = function(context, con, key_alive = NULL, worker_name = NULL,
                          time_poll = NULL, log_path = NULL, timeout = NULL,
                          heartbeat_period = NULL) {
      assert_is(context, "context")
      assert_is(con, "redis_api")
      self$context <- context
      self$con <- con
      self$db <- context$db

      self$name <- worker_name %||% ids::adjective_animal()
      self$keys <- rrq_keys(context$id, self$name)

      self$time_poll <- time_poll %||% 60
      self$cores <- as.integer(Sys.getenv("CONTEXT_CORES", "0"))

      self$log_path <- worker_initialise_logs(self$context, log_path)

      if (self$con$SISMEMBER(self$keys$worker_name, self$name) == 1L) {
        stop("Looks like this worker exists already...")
      }

      withCallingHandlers(
        worker_initialise(self, key_alive, timeout, heartbeat_period),
        error = worker_catch_error(self))
    },

    info = function() {
      worker_info_collect(self)
    },

    log = function(label, value = NULL) {
      res <- worker_log_format(label, value)
      message(res$screen)
      self$con$RPUSH(self$keys$worker_log, res$redis)
    },

    load_context = function() {
      e <- new.env(parent = .GlobalEnv)
      self$context <- context::context_load(self$context, e, refresh = TRUE)
      self$envir <- self$context$envir
    },

    poll = function(immediate = FALSE) {
      keys <- self$keys
      if (self$paused) {
        keys <- keys$worker_message
      } else {
        keys <- c(keys$worker_message, keys$queue_rrq, keys$queue_ctx)
      }
      self$loop_task <- blpop(self$con, keys, self$time_poll, immediate)
    },

    step = function(immediate = FALSE) {
      worker_step(self, immediate)
    },

    loop = function() {
      worker_loop(self)
    },

    run_task = function(task_id, rrq) {
      worker_run_task(self, task_id, rrq)
    },

    run_message = function(msg) {
      run_message(self, msg)
    },

    format = function() {
      worker_format(self)
    },

    shutdown = function(status = "OK", graceful = TRUE) {
      if (!is.null(self$heartbeat)) {
        context::context_log("heartbeat", "stopping")
        tryCatch(
          self$heartbeat$stop(graceful),
          error = function(e) message("Could not stop heartbeat"))
      }
      if (self$cores > 0) {
        context::parallel_cluster_stop()
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
              wd = getwd(),
              pid = process_id(),
              redis_host = redis_config$host,
              redis_port = redis_config$port,
              context_id = worker$context$id,
              context_root = worker$context$root$path,
              log_path = worker$log_path)
  if (!is.null(worker$heartbeat)) {
    dat$heartbeat_key <- worker$keys$worker_heartbeat
  }
  dat
}


worker_initialise <- function(worker, key_alive, timeout, heartbeat_period) {
  keys <- worker$keys

  context::context_log_start()
  worker$load_context()

  worker$heartbeat <- heartbeat(worker$con, keys$worker_heartbeat,
                                heartbeat_period)

  worker$con$pipeline(
    redis$SADD(keys$worker_name,   worker$name),
    redis$HSET(keys$worker_status, worker$name, WORKER_IDLE),
    redis$HDEL(keys$worker_task,   worker$name),
    redis$DEL(keys$worker_log),
    redis$HSET(keys$worker_info,   worker$name, object_to_bin(worker$info())))

  if (worker$cores > 0) {
    context::context_log("parallel",
                         sprintf("running as parallel job [%d cores]",
                                 worker$cores))
    context::parallel_cluster_start(worker$cores, worker$context)
  }

  if (!is.null(timeout)) {
    run_message_TIMEOUT_SET(worker, timeout)
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
    for (key in rrq_key_worker_heartbeat(keys$queue_name, worker_ids)) {
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
      worker$run_task(task[[2L]], task[[1L]] == keys$queue_rrq)
      worker$timer <- NULL
    }
  }

  if (is.null(task) && !is.null(worker$timeout)) {
    if (is.null(worker$timer)) {
      worker$timer <- queuer::time_checker(worker$timeout, remaining = TRUE)
    }
    if (worker$timer() < 0L) {
      stop(rrq_worker_stop(worker, "TIMEOUT"))
    }
  }
}


worker_loop <- function(worker, verbose = TRUE) {
  if (verbose) {
    message(paste0(worker_banner("start"), collapse = "\n"))
    message(paste0(worker$format(), collapse = "\n"))
  }

  worker$loop_continue <- TRUE
  while (worker$loop_continue) {
    tryCatch({
      tryCatch(worker$step(),
               interrupt = worker_catch_interrupt(worker))
    },
    rrq_worker_stop = worker_catch_stop(worker),
    error = worker_catch_error(worker))
  }
  if (verbose) {
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
    c("message", "rrq", "context"),
    c(worker$keys$worker_message,
      worker$keys$queue_rrq,
      worker$keys$queue_ctx))

  function(e) {
    ## NOTE: this won't recursively catch interrupts.  Especially
    ## on a high-latency connection this might be long enough for
    ## a second interrupt to arrive.  We don't deal with that and
    ## it will be about the same as a SIGTERM - we'll just die.
    ## But that will disable the heartbeat so it should all end up
    ## OK.
    worker$log("INTERRUPT")

    task_running <- worker$con$HGET(worker$keys$worker_task, worker$name)
    if (!is.null(task_running)) {
      worker_run_task_cleanup(worker, task_running, TASK_INTERRUPTED)
    }

    ## There are two ways that interrupt happens (ignoring the
    ## race condition where it comes in neither)
    ##
    ## 1. Interrupt a job; in that case, we have a task_running
    ##    above and everything is all OK; we just mark this as done.
    ##
    ## 2. During BLPOP.  In that case we need to re-queue the
    ##    message or the job or it is lost from the queue
    ##    entirely.  This would include things like a STOP
    ##    message, or a new task to run.
    ##
    ## NOTE that a SIGTERM signal might result in lost messages.
    task <- worker$loop_task
    if (!is.null(task[[2]]) && !identical(task[[2]], task_running)) {
      label <- sprintf("%s:%s", queue_type[[task[[1]]]], task[[2]])
      worker$log("REQUEUE", label)
      worker$con$LPUSH(task[[1]], task[[2]])
    }
  }
}


worker_initialise_logs <- function(context, path) {
  if (!is.null(path)) {
    if (!is_relative_path(path)) {
      stop("Must be a relative path")
    }
    dir.create(file.path(context$root$path, path), FALSE, TRUE)
    path
  }
}


worker_format <- function(worker) {
  x <- worker$info()
  x$log_path <- x$log_path %||% "<not set>"
  x$heartbeat_key <- x$heartbeat_key %||% "<not set>"
  n <- nchar(names(x))
  pad <- vcapply(max(n) - n, strrep, x = " ")
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
