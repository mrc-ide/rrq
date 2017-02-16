##' Create a "worker controller" object, which can deal with
##' coordinating rrq workers.  This is subject to change.
##'
##' @title Create a worker controller
##'
##' @param context_id A context ID; this will not be checked for
##'   existance, and no root is needed (everything this object does
##'   affects only the Redis database).
##'
##' @param con A Redis connection (optional; the default connection
##'   will be used otherwise).
##'
##' @export
worker_controller <- function(context_id, con = redux::hiredis()) {
  R6_worker_controller$new(context_id, con)
}

R6_worker_controller <- R6::R6Class(
  "worker_controller",
  public = list(
    con = NULL,
    keys = NULL,
    initialize = function(context_id, con) {
      self$con <- con
      self$keys <- rrq_keys(context_id)
      push_controller_info(self$con, self$keys)
    },

    destroy = function(delete = TRUE, type = "message") {
      rrq_clean(self$con, self$keys$queue_name, delete, type)
      ## render the controller useless:
      self$con <- NULL
      self$keys <- NULL
    },

    ## These ones probably aren't totally needed, but are quite nice
    ## to have:
    queue_length = function() {
      self$con$LLEN(self$keys$queue_ctx)
    },
    queue_list = function() {
      as.character(self$con$LRANGE(self$keys$queue_ctx, 0, -1))
    },

    ## These ones do the actual submission
    queue_submit = function(task_ids) {
      self$con$RPUSH(self$keys$queue_ctx, task_ids)
    },
    queue_unsubmit = function(task_ids) {
      ## NOTE: probable race condition, consider rename, which has bad
      ## properties if the queue is lost of course.  The correct way
      ## would be to do a lua script.
      ids_queued <- self$queue_list()
      if (length(ids_queued) > 0L) {
        self$con$DEL(self$keys$queue_ctx)
        ids_keep <- setdiff(ids_queued, task_ids)
        if (length(ids_keep) > 0L) {
          self$con$RPUSH(self$keys$queue_ctx, ids_keep)
        }
      }
    },

    ## The messaging system from rrqueue, verbatim:
    send_message = function(command, args = NULL, worker_ids = NULL) {
      send_message(self$con, self$keys, command, args, worker_ids)
    },
    has_responses = function(message_id, worker_ids = NULL) {
      has_responses(self$con, self$keys, message_id, worker_ids)
    },
    has_response = function(message_id, worker_id) {
      has_response(self$con, self$keys, message_id, worker_id)
    },
    get_responses = function(message_id, worker_ids = NULL, delete = FALSE,
                             timeout = 0, time_poll = 1, progress = TRUE) {
      get_responses(self$con, self$keys, message_id, worker_ids, delete,
                    timeout, time_poll, progress)
    },
    get_response = function(message_id, worker_id, delete = FALSE,
                            timeout = 0, time_poll = 1, progress = TRUE) {
      get_response(self$con, self$keys, message_id, worker_id, delete,
                   timeout, time_poll, progress)
    },
    response_ids = function(worker_id) {
      response_ids(self$con, self$keys, worker_id)
    },

    ## All in one:
    send_message_and_wait = function(command, args = NULL, worker_ids = NULL,
                                     delete = TRUE, timeout = 600,
                                     time_poll = 0.05, progress = TRUE) {
      send_message_and_wait(self$con, self$keys, command, args, worker_ids,
                            delete, timeout, time_poll, progress)
    },

    ## Query workers:
    workers_len = function() {
      workers_len(self$con, self$keys)
    },
    workers_list = function() {
      workers_list(self$con, self$keys)
    },
    workers_list_exited = function() {
      workers_list_exited(self$con, self$keys)
    },
    workers_info = function(worker_ids = NULL) {
      workers_info(self$con, self$keys, worker_ids)
    },
    workers_status = function(worker_ids = NULL) {
      workers_status(self$con, self$keys, worker_ids)
    },
    workers_log_tail = function(worker_ids = NULL, n = 1) {
      workers_log_tail(self$con, self$keys, worker_ids, n)
    },
    workers_task_id = function(worker_ids = NULL) {
      workers_task_id(self$con, self$keys, worker_ids)
    },
    workers_delete_exited = function(worker_ids = NULL) {
      workers_delete_exited(self$con, self$keys, worker_ids)
    },

    workers_stop = function(worker_ids = NULL, type = "message",
                            timeout = 0, time_poll = 1, progress = TRUE) {
      workers_stop(self$con, self$keys, worker_ids, type,
                   timeout, time_poll, progress)
    }
  ))

workers_len <- function(con, keys) {
  con$SCARD(keys$workers_name)
}
workers_list <- function(con, keys) {
  as.character(con$SMEMBERS(keys$workers_name))
}

workers_list_exited <- function(con, keys) {
  setdiff(as.character(con$HKEYS(keys$workers_info)), workers_list(con, keys))
}

workers_status <- function(con, keys, worker_ids = NULL) {
  from_redis_hash(con, keys$workers_status, worker_ids)
}

workers_info <- function(con, keys, worker_ids = NULL) {
  from_redis_hash(con, keys$workers_info, worker_ids,
                  f = Vectorize(bin_to_object, SIMPLIFY = FALSE))
}

workers_log_tail <- function(con, keys, worker_ids = NULL, n = 1) {
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  tmp <- lapply(worker_ids, function(i) worker_log_tail(con, keys, i, n))
  if (length(tmp) > 0L) {
    n <- viapply(tmp, nrow)
    ret <- cbind(worker_id = rep(worker_ids, n),
                 do.call("rbind", tmp, quote = TRUE))
    ret <- ret[order(ret$time, ret$worker_id), ]
    rownames(ret) <- NULL
    ret
  } else {
    ## NOTE: Need to keep this in sync with parse_worker_log; get some
    ## tests in here to make sure...
    data.frame(worker_id = worker_ids, time = character(0),
               command = character(0), message = character(0),
               stringsAsFactors = FALSE)
  }
}

worker_log_tail <- function(con, keys, worker_id, n = 1) {
  ## More intuitive `n` behaviour for "print all entries"; n of Inf
  if (identical(n, Inf)) {
    n <- 0
  }
  log_key <- rrq_key_worker_log(keys$queue_name, worker_id)
  log <- as.character(con$LRANGE(log_key, -n, -1))
  re <- "^([0-9]+) ([^ ]+) ?(.*)$"
  if (!all(grepl(re, log))) {
    stop("Corrupt log")
  }
  time <- as.integer(sub(re, "\\1", log))
  command <- sub(re, "\\2", log)
  message <- lstrip(sub(re, "\\3", log))
  data.frame(time, command, message, stringsAsFactors = FALSE)
}

workers_task_id <- function(con, keys, worker_id) {
  from_redis_hash(con, keys$workers_task, worker_id)
}

workers_delete_exited <- function(con, keys, worker_ids = NULL) {
  ## This only includes things that have been processed and had task
  ## orphaning completed.
  if (is.null(worker_ids)) {
    worker_ids <- workers_list_exited(con, keys)
  } else {
    extra <- setdiff(worker_ids, workers_list_exited(con, keys))
    if (length(extra)) {
      stop(sprintf(
        ## TODO: this function does not exist!  It's in rrqueue and I
        ## think depends on heartbeat.
        "Workers %s may not have exited;\n\trun workers_identify_lost first",
        paste(extra, collapse = ", ")))
    }
  }
  if (length(worker_ids) > 0L) {
    con$HDEL(keys$workers_name,   worker_ids)
    con$HDEL(keys$workers_status, worker_ids)
    con$HDEL(keys$workers_task,   worker_ids)
    con$HDEL(keys$workers_info,   worker_ids)
    con$DEL(c(rrq_key_worker_log(keys$queue_name, worker_ids),
              rrq_key_worker_message(keys$queue_name, worker_ids),
              rrq_key_worker_response(keys$queue_name, worker_ids)))
  }
  worker_ids
}

workers_stop <- function(con, keys, worker_ids = NULL, type = "message",
                         timeout = 0, time_poll = 1, progress = TRUE) {
  type <- match.arg(type, c("message", "kill", "kill_local"))
  if (is.null(worker_ids)) {
    worker_ids <- workers_list(con, keys)
  }
  if (length(worker_ids) == 0L) {
    return(invisible(worker_ids))
  }

  if (type == "message") {
    ## First, we send a message saying 'STOP'
    message_id <- send_message(con, keys, "STOP", worker_ids = worker_ids)
    ## ## TODO: This needs RedisHeartbeat support
    ##
    ## if (interrupt) {
    ##   is_busy <- workers_status(con, keys, worker_ids) == WORKER_BUSY
    ##   if (any(is_busy)) {
    ##     queue_send_signal(con, keys, tools::SIGINT, worker_ids[is_busy])
    ##   }
    ## }
    if (timeout > 0L) {
      ok <- try(get_responses(con, keys, message_id, worker_ids,
                              delete = FALSE, timeout = timeout,
                              time_poll = time_poll, progress = progress),
                silent = TRUE)
      ## if (inherits(ok, "try-error")) {
      ##   done <- has_responses(con, keys, message_id, worker_ids)
      ##   workers_stop(con, keys, worker_ids[!done], "kill")
      ## }
    }
    ## TODO: This requires RedisHeartbeat support
  } else if (type == "kill") {
    ## queue_send_signal(con, keys, tools::SIGTERM, worker_ids)
    stop("Needs redis heartbeat support")
  } else {
    ## kill_local
    w_info <- workers_info(con, keys, worker_ids)
    w_local <- vcapply(w_info, "[[", "hostname") == hostname()
    if (!all(w_local)) {
      stop("Not all workers are local: ",
           paste(worker_ids[!w_local], collapse = ", "))
    }
    tools::pskill(vnapply(w_info, "[[", "pid"), tools::SIGTERM)
  }
  invisible(worker_ids)
}
