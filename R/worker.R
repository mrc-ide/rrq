## TODO: decide if workers clean up on exit.
##
## TODO: decide if there is an implicit timeout (e.g., an hour)

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
##'   root (though perhaps later this will be sanitised to support
##'   absolute paths...).
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
  R6_rrq_worker$new(context, con, key_alive, worker_name, time_poll,
                     log_path, timeout, heartbeat_period)
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

    initialize = function(context, con, key_alive, worker_name, time_poll,
                          log_path, timeout, heartbeat_period) {
      assert_is(context, "context")
      assert_is(con, "redis_api")
      self$context <- context
      self$con <- con
      self$db <- context$db

      self$name <- worker_name %||% ids::adjective_animal()
      self$keys <- rrq_keys(context$id, self$name)

      self$time_poll <- time_poll %||% 60

      if (!is.null(log_path)) {
        if (!is_relative_path(log_path)) {
          stop("Must be a relative path")
        }
        self$log_path <- log_path
        dir.create(file.path(self$context$root$path, self$log_path),
                   FALSE, TRUE)
      }

      self$load_context()

      ## NOTE: This needs to happen *before* running the
      ## initialize_worker; it checks that we can actually use the
      ## database and that we will not write anything to an existing
      ## worker.  An error here will not be caught.
      ##
      ## TODO: Provide some guidance as to what to do here!
      if (self$con$SISMEMBER(self$keys$worker_name, self$name) == 1L) {
        stop("Looks like this worker exists already...")
      }

      ## NOTE: this could be moved into the main initialise_worker
      ## function but I don't really think that it fits there.  This
      ## function is allowed to throw.
      self$heartbeat <- heartbeat(self$con, self$keys$worker_heartbeat,
                                  heartbeat_period)

      withCallingHandlers(self$initialise_worker(key_alive),
                          error = self$catch_error)

      cores <- Sys.getenv("CONTEXT_CORES")
      if (nzchar(cores)) {
        cores <- as.integer(cores)
        context::context_log("parallel",
                             sprintf("running as parallel job [%d cores]",
                                     cores))
        context::parallel_cluster_start(cores, self$context)
        on.exit(context::parallel_cluster_stop())
      } else {
        context::context_log("parallel", "running as single core job")
      }

      if (!is.null(timeout)) {
        run_message_TIMEOUT_SET(self, timeout)
      }

      self$main_loop()
      message(worker_exit_text())
    },

    load_context = function() {
      e <- new.env(parent = .GlobalEnv)
      self$context <- context::context_load(self$context, e, refresh = TRUE)
      self$envir <- self$context$envir
    },

    initialise_worker = function(key_alive) {
      info <- object_to_bin(self$print_info())
      keys <- self$keys

      self$con$pipeline(
        redis$SADD(keys$worker_name,   self$name),
        redis$HSET(keys$worker_status, self$name, WORKER_IDLE),
        redis$HDEL(keys$worker_task,   self$name),
        redis$DEL(keys$worker_log),
        redis$HSET(keys$worker_info,   self$name, info))
      self$log("ALIVE")

      ## This announces that we're up; things may monitor this
      ## queue, and worker_spawn does a BLPOP to
      if (!is.null(key_alive)) {
        self$con$RPUSH(key_alive, self$name)
      }
    },

    ## TODO: if we're not running in a terminal, then we should output
    ## the worker id into the screen message.
    log = function(label, message = NULL, push = TRUE) {
      t <- Sys.time()
      ts <- as.character(t)
      td <- sprintf("%.04f", t) # to nearest 1/10000 s
      if (is.null(message)) {
        msg_log <- sprintf("%s %s", td, label)
        msg_scr <- sprintf("[%s] %s", ts, label)
      } else {
        msg_log <- sprintf("%s %s %s",
                           td, label, paste(message, collapse = "\n"))
        ## Try and make nicely printing logs for the case where the
        ## message length is longer than 1:
        lab <- c(label, rep_len(blank(nchar(label)), length(message) - 1L))
        msg_scr <- paste(sprintf("[%s] %s %s", ts, lab, message),
                         collapse = "\n")
      }
      message(msg_scr)
      if (push) {
        self$con$RPUSH(self$keys$worker_log, msg_log)
      }
    },

    main_loop = function() {
      con <- self$con
      keys <- self$keys
      continue <- TRUE
      task <- NULL # scoped variable
      listen_message <- keys$worker_message
      listen <- c(listen_message, keys$queue_rrq, keys$queue_ctx)
      time_poll <- self$time_poll

      catch_worker_stop <- function(e) {
        self$shutdown(sprintf("OK (%s)", e$message), TRUE)
        continue <<- FALSE
      }
      catch_interrupt <- function(e) {
        ## NOTE: this won't recursively catch interrupts.  Especially
        ## on a high-latency connection this might be long enough for
        ## a second interrupt to arrive.  We don't deal with that and
        ## it will be about the same as a SIGTERM - we'll just die.
        ## But that will disable the heartbeat so it should all end up
        ## OK.
        self$log("INTERRUPT")
        ## This condition is not quite enough; I need to know if we're
        ## working on a job at all.
        task_running <- self$con$HGET(self$keys$worker_task, self$name)
        if (!is.null(task_running)) {
          self$task_cleanup(e, task_running, TASK_INTERRUPTED)
          con$HSET(keys$task_status, task_running, TASK_INTERRUPTED)
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
        if (!is.null(task[[2]]) && !identical(task[[2]], task_running)) {
          label <- if (task[[1L]] == listen_message) "<message>" else task[[2L]]
          self$log("REQUEUE", label)
          self$con$LPUSH(task[[1]], task[[2]])
        }
      }
      loop <- function() {
        while (continue) {
          task <<- con$BLPOP(if (self$paused) listen_message else listen,
                             time_poll)
          if (!is.null(task)) {
            if (task[[1L]] == listen_message) {
              self$run_message(task[[2L]])
            } else {
              self$run_task(task[[2L]], task[[1L]] == keys$queue_rrq)
              self$timer <- NULL
              next # don't check timeouts when processing tasks...
            }
          }
          if (!is.null(self$timeout)) {
            if (is.null(task) && is.null(self$timer)) {
              self$timer <- queuer::time_checker(self$timeout, TRUE)
            }
            if (is.function(self$timer) && self$timer() < 0L) {
              stop(rrq_worker_stop(self, "TIMEOUT"))
            }
          }
        }
      }

      while (continue) {
        tryCatch(loop(),
                 interrupt = catch_interrupt,
                 rrq_worker_stop = catch_worker_stop,
                 error = self$catch_error)
      }
    },

    run_task = function(task_id, rrq) {
      self$log("TASK_START", task_id)
      con <- self$con
      keys <- self$keys

      con$pipeline(
        redis$HSET(keys$worker_status, self$name, WORKER_BUSY),
        redis$HSET(keys$worker_task,   self$name, task_id),
        redis$HSET(keys$task_worker,   task_id,   self$name),
        redis$HSET(keys$task_status,   task_id,   TASK_RUNNING))
      ## Run the task:
      if (rrq) {
        self$run_task_rrq(task_id)
      } else {
        self$run_task_context(task_id)
      }
    },

    run_task_rrq = function(task_id) {
      keys <- self$keys
      con <- self$con

      dat <- bin_to_object(con$HGET(keys$task_expr, task_id))
      e <- context::restore_locals(dat, self$envir, self$db)

      cl <- c("rrq_task_error", "try-error")
      res <- context:::eval_safely(dat$expr, e, cl, 3L)
      value <- res$value

      task_status <- if (res$success) TASK_COMPLETE else TASK_ERROR
      con$HSET(keys$task_result, task_id, object_to_bin(value))
      con$HSET(keys$task_status, task_id, task_status)

      self$task_cleanup(value, task_id, task_status)
    },

    run_task_context = function(task_id) {
      if (is.null(self$log_path)) {
        log_file <- NULL
      } else {
        log_path <- file.path(self$log_path, paste0(task_id, ".log"))
        self$db$set(task_id, log_path, "log_path")
        log_file <- file.path(self$context$root$path, log_path)
      }
      res <- context::task_run(task_id, self$context, log_file)
      task_status <-
        if (inherits(res, "context_task_error")) TASK_ERROR else TASK_COMPLETE
      self$task_cleanup(res, task_id, task_status)
    },

    task_cleanup = function(res, task_id, task_status) {
      con <- self$con
      keys <- self$keys
      key_complete <- con$HGET(keys$task_complete, task_id)

      ## TODO: I should enforce a max size policy here.  So if the
      ## return value is too large (say more than a few kb) we can
      ## refuse to write it to the db but instead route it through the
      ## context db.  That policy can be set by the db pretty easily
      ## actually.
      ##
      ## TODO: for interrupted tasks, I don't know that we should
      ## write to the key_complete set; at the same time _not_ doing
      ## that requires that we can gracefully (and automatically)
      ## handle the restarts.
      con$pipeline(
        redis$HINCRBY(keys$task_status, task_status, 1),
        redis$HSET(keys$worker_status, self$name, WORKER_IDLE),
        redis$HDEL(keys$worker_task,   self$name),
        if (!is.null(key_complete)) { # && task_status != TASK_ORPHAN
          redis$RPUSH(key_complete, task_id)
        })

      self$log(paste0("TASK_", task_status), task_id)
    },

    run_message = function(msg) {
      run_message(self, msg)
    },

    send_response = function(message_id, cmd, result) {
      self$log("RESPONSE", cmd)
      self$con$HSET(self$keys$worker_response, message_id,
                    response_prepare(message_id, cmd, result))
    },

    print_info = function() {
      print(worker_info_collect(self), banner = TRUE, styles = self$styles)
    },

    catch_error = function(e) {
      self$shutdown("ERROR", FALSE)
      message("This is an uncaught error in rrq, probably a bug!")
      stop(e)
    },

    shutdown = function(status = "OK", graceful = TRUE) {
      if (!is.null(self$heartbeat)) {
        context::context_log("heartbeat", "stopping")
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
              wd = getwd(),
              pid = process_id(),
              redis_host = redis_config$host,
              redis_port = redis_config$port,
              context_id = worker$context$id,
              context_root = worker$context$root$path,
              log_path = worker$log_path)
  if (!is.null(worker$heartbeat)) {
    dat$heartbeat_key = worker$keys$worker_heartbeat
  }
  class(dat) <- "worker_info"
  dat
}

##' @export
print.worker_info <- function(x, banner = FALSE, ...) {
  xx <- unclass(x)
  xx$log_path <- x$log_path %||% "<not set>"
  xx$heartbeat_key <- x$heartbeat_key %||% "<not set>"
  n <- nchar(names(xx))
  pad <- vcapply(max(n) - n, strrep, x = " ")
  ret <- sprintf("    %s:%s %s", names(xx), pad, as.character(xx))
  if (banner) {
    message(worker_banner_text())
  }
  message(paste(ret, collapse = "\n"))
  invisible(x)
}

## To regenerate / change:
##   fig <- rfiglet::figlet(sprintf("_- %s -_", "rrq!"), "slant")
##   dput(rstrip(strsplit(as.character(fig), "\n")[[1]]))
worker_banner_text <- function() {
  c("                                 __",
    "                ______________ _/ /",
    "      ______   / ___/ ___/ __ `/ /  ______",
    "     /_____/  / /  / /  / /_/ /_/  /_____/",
    " ______      /_/  /_/   \\__, (_)      ______",
    "/_____/                   /_/        /_____/") -> txt
  paste(txt, collapse = "\n")
}

## To regenerate / change:
##   fig <- rfiglet::figlet(sprintf("bye bye for now"), "smslant")
##   dput(sub("\\s+$", "", (strsplit(as.character(fig), "\n")[[1]])))
worker_exit_text <- function() {
  c("   __              __              ___",
    "  / /  __ _____   / /  __ _____   / _/__  ____  ___  ___ _    __",
    " / _ \\/ // / -_) / _ \\/ // / -_) / _/ _ \\/ __/ / _ \\/ _ \\ |/|/ /",
    "/_.__/\\_, /\\__/ /_.__/\\_, /\\__/ /_/ \\___/_/   /_//_/\\___/__,__/",
    "     /___/           /___/") -> txt
  paste(txt, collapse = "\n")
}

## Controlled stop:
rrq_worker_stop <- function(worker, message) {
  structure(list(worker = worker,
                 message = message),
            class = c("rrq_worker_stop", "error", "condition"))
}

worker_send_signal <- function(con, keys, signal, worker_ids) {
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }
  if (length(worker_ids) > 0L) {
    for (key in rrq_key_worker_heartbeat(keys$queue_name, worker_ids)) {
      heartbeatr::heartbeat_send_signal(con, key, signal)
    }
  }
}
