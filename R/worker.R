## TODO: decide if workers clean up on exit.
WORKER_IDLE <- "IDLE"
WORKER_BUSY <- "BUSY"
WORKER_EXITED <- "EXITED"
WORKER_LOST <- "LOST"
WORKER_PAUSED <- "PAUSED"

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
##' @export
rrq_worker <- function(context, con, key_alive=NULL, worker_name=NULL,
                       time_poll=60) {
  .R6_rrq_worker$new(context, con, key_alive, worker_name, time_poll)
  invisible()
}

.R6_rrq_worker <- R6::R6Class(
  "rrq_worker",
  public=list(
    name=NULL,
    context=NULL,
    envir=NULL,
    keys=NULL,
    con=NULL,
    db=NULL,
    paused=FALSE,
    time_poll=NULL,

    initialize=function(context, con, key_alive, worker_name, time_poll) {
      self$context <- context
      self$con <- con

      self$db <- context::context_db(context)

      self$name <- worker_name %||% ids::random_id()
      self$keys <- rrq_keys(context$id, self$name)

      self$time_poll <- time_poll

      self$load_context()

      ## NOTE: This needs to happen *before* running the
      ## initialize_worker; it checks that we can actually use the
      ## database and that we will not write anything to an existing
      ## worker.  An error here will not be caught.
      ##
      ## TODO: Provide some guidance as to what to do here!
      if (self$con$SISMEMBER(self$keys$workers_name, self$name) == 1L) {
        stop("Looks like this worker exists already...")
      }

      withCallingHandlers(self$initialise_worker(key_alive),
                          error=self$catch_error)
      self$main_loop()
      message(worker_exit_text())
    },

    load_context=function() {
      self$envir <- context::context_load(self$context)
    },

    initialise_worker=function(key_alive) {
      info <- object_to_bin(self$print_info())
      keys <- self$keys

      redux::redis_multi(self$con, {
        self$con$SADD(keys$workers_name,   self$name)
        self$con$HSET(keys$workers_status, self$name, WORKER_IDLE)
        self$con$HDEL(keys$workers_task,   self$name)
        self$con$DEL(keys$log)
        self$con$HSET(keys$workers_info,   self$name, info)
      })
      self$log("ALIVE")

      ## This announces that we're up; things may monitor this
      ## queue, and workers_spawn does a BLPOP to
      if (!is.null(key_alive)) {
        self$con$RPUSH(key_alive, self$name)
      }
    },

    ## TODO: if we're not running in a terminal, then we should output
    ## the worker id into the screen message.
    log=function(label, message=NULL, push=TRUE) {
      t <- Sys.time()
      ti <- as.integer(t) # to nearest second
      ts <- as.character(t)
      if (is.null(message)) {
        msg_log <- sprintf("%d %s", ti, label)
        msg_scr <- sprintf("[%s] %s", ts, label)
      } else {
        msg_log <- sprintf("%d %s %s", ti, label, paste(message, collapse="\n"))
        ## Try and make nicely printing logs for the case where the
        ## message length is longer than 1:
        lab <- c(label, rep_len(blank(nchar(label)), length(message) - 1L))
        msg_scr <- paste(sprintf("[%s] %s %s", ts, lab, message),
                         collapse="\n")
      }
      message(msg_scr)
      if (push) {
        self$con$RPUSH(self$keys$log, msg_log)
      }
    },

    main_loop=function() {
      con <- self$con
      keys <- self$keys
      continue <- TRUE
      listen_message <- keys$message
      listen <- c(listen_message, keys$tasks_queue)
      time_poll <- self$time_poll

      catch_worker_stop <- function(e) {
        self$shutdown("OK")
        continue <<- FALSE
      }
      catch_worker_error <- function(e) {
        if (!is.null(e$task_id)) {
          self$task_cleanup(e, e$task_id, e$task_status)
        }
      }

      while (continue) {
        tryCatch({
          task <- con$BLPOP(listen, time_poll)
          if (!is.null(task)) {
            if (task[[1]] == listen_message) {
              self$run_message(task[[2]])
            } else { # is a task
              self$run_task(task[[2]])
            }
          }
        },
        WorkerStop=catch_worker_stop,
        WorkerError=catch_worker_error,
        error=self$catch_error)
      }
    },

    run_task=function(task_id) {
      keys <- self$keys
      con <- self$con
      db <- self$db
      e <- new.env(parent=self$envir)

      self$log("TASK_START", task_id)

      ## TODO: I don't see how locals are going to work.  So when we
      ## pass in a big sod-off matrix, etc.  Probably we can serialise
      ## them into the context rather than passing them through the
      ## db.
      ##
      dat <- con$HGET(keys$tasks_expr, task_id)
      expr <- restore_expression(bin_to_object(dat), e, db)

      redux::redis_multi(con, {
        con$HSET(keys$workers_status, self$name, WORKER_BUSY)
        con$HSET(keys$workers_task,   self$name, task_id)
        con$HSET(keys$tasks_worker,   task_id,   self$name)
        con$HSET(keys$tasks_status,   task_id,   TASK_RUNNING)
      })

      res <- tryCatch(eval(expr, e),
                      error=WorkerTaskError)

      if (inherits(res, "WorkerTaskError")) {
        task_status <- TASK_ERROR
      } else {
        task_status <- TASK_COMPLETE
      }

      self$task_cleanup(res, task_id, task_status)
    },

    task_cleanup=function(res, task_id, task_status) {
      con <- self$con
      keys <- self$keys
      key_complete <- con$HGET(keys$tasks_complete, task_id)

      ## TODO: I should enforce a max size policy here.  So if the
      ## return value is too large (say more than a few kb) we can
      ## refuse to write it to the db but instead route it through the
      ## context db.  That policy can be set by the db pretty easily
      ## actually.
      con$HSET(keys$tasks_result,   task_id,   object_to_bin(res))
      con$HSET(keys$tasks_status,   task_id,   task_status)
      con$HSET(keys$workers_status, self$name, WORKER_IDLE)
      con$HDEL(keys$workers_task,   self$name)
      if (!is.null(key_complete)) {
        con$RPUSH(key_complete, task_id)
      }

      self$log(paste0("TASK_", task_status), task_id)
    },

    run_message=function(msg) {
      ## TODO: these can be unserialised...
      content <- bin_to_object(msg)
      message_id <- content$id
      cmd <- content$command
      args <- content$args

      self$log("MESSAGE", cmd)

      ## TODO: worker restart?  Is that even possible?
      res <- switch(cmd,
                    PING=run_message_PING(),
                    ECHO=run_message_ECHO(args),
                    EVAL=run_message_EVAL(args),
                    STOP=run_message_STOP(self, message_id, args), # noreturn
                    INFO=run_message_INFO(self),
                    PAUSE=run_message_PAUSE(self, args),
                    RESUME=run_message_RESUME(self, args),
                    ## RELOAD=run_message_RELOAD(self),
                    ## FINISH=run_message_FINISH(self, args),
                    run_message_unknown(cmd, args))

      self$send_response(message_id, cmd, res)
    },

    send_response=function(message_id, cmd, result) {
      self$log("RESPONSE", cmd)
      self$con$HSET(self$keys$response, message_id,
                    response_prepare(message_id, cmd, result))
    },

    print_info=function() {
      print(worker_info(self), banner=TRUE, styles=self$styles)
    },

    catch_error=function(e) {
      self$shutdown("ERROR")
      message("This is an uncaught error in rrqueue, probably a bug!")
      stop(e)
    },

    shutdown=function(status="OK") {
      ## Conditional here because the heartbeat can fail to start, in
      ## which case we can't run the heartbeat shutdown.
      ## if (!is.null(self$heartbeat$stop)) {
      ##   self$heartbeat$stop()
      ## }
      ## self$con$DEL(self$keys$heartbeat)
      self$con$SREM(self$keys$workers_name,   self$name)
      self$con$HSET(self$keys$workers_status, self$name, WORKER_EXITED)
      self$log("STOP", status)
    }
  ))

worker_info <- function(worker) {
  sys <- sessionInfo()
  redis_config <- worker$con$config()
  dat <- list(version=version_string(),
              platform=sys$platform,
              running=sys$running,
              hostname=hostname(),
              pid=process_id(),
              redis_host=redis_config$host,
              redis_port=redis_config$port,
              worker=worker$name,
              time_poll=worker$time_poll,
              context_id=worker$context$id,
              context_root=worker$context$root,
              message=worker$keys$message,
              response=worker$keys$response,
              log=worker$keys$log)
  class(dat) <- "worker_info"
  dat
}

##' @export
print.worker_info <- function(x, banner=FALSE, ...) {
  xx <- x
  n <- nchar(names(xx))
  pad <- vcapply(max(n) - n, strrep, x=" ")
  ret <- sprintf("    %s:%s %s", names(xx), pad, as.character(xx))
  if (banner) {
    message(worker_banner_text())
  }
  message(paste(ret, collapse="\n"))
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
  paste(txt, collapse="\n")
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
  paste(txt, collapse="\n")
}

WorkerError <- function(worker, message, ...,
                        task_id=NULL,
                        task_status=NULL,
                        class=character(0),
                        call=NULL) {
  structure(list(worker=worker,
                 task_id=task_id,
                 task_status=task_status, ...,
                 message=message, call=call),
            class=c(class, "WorkerError", "error", "condition"))
}

WorkerStop <- function(worker, message) {
  WorkerError(worker, message, class="WorkerStop")
}

WorkerTaskError <- function(e) {
  class(e) <- c("WorkerTaskError", "try-error", class(e))
  e
}

WorkerTaskMissing <- function(worker, task_id) {
  msg <- sprintf("Task %s/%s not found", worker$name, task_id)
  worker$log("TASK_MISSING", task_id)
  WorkerError(worker, msg,
              task_id=task_id, task_status=TASK_MISSING,
              class="WorkerTaskMissing")
}

workers_list <- function(con, keys) {
  as.character(con$SMEMBERS(keys$workers_name))
}

workers_list_exited <- function(con, keys) {
  setdiff(as.character(con$HKEYS(keys$workers_info)), workers_list(con, keys))
}

workers_status <- function(con, keys, worker_ids=NULL) {
  from_redis_hash(con, keys$workers_status, worker_ids)
}
