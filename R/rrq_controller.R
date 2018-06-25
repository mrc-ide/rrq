##' A queue controller.  Use this to interact with a queue/cluster.
##' @title rrq queue controller
##'
##' @param context A context handle object.  The context needs to be
##'   loaded.  Alternatively, just the context id can be passed
##'   through here (not the context object itself).  In that case,
##'   some communication is disabled (we will not have access to
##'   context's internal database, nor the filesystem (of course it
##'   does sort of have access via the workers if there are any).
##'   Currently this impacts \code{worker_process_log} (which needs
##'   access to the filesystem), \code{worker_config_save} (which
##'   needs access to both the filesystem and the context database),
##'   and \code{lapply} and \code{enqueue_bulk} which both need access
##'   to the \emph{loaded} context, at least for now.
##'
##' @param con A redis connection (redux object).
##'
##' @export
rrq_controller <- function(context, con = redux::hiredis()) {
  if (is.character(context)) {
    ## TODO: we could probably lift the context out of the loaded set
    ## in some cases, once context exposes that.
    context_id <- context
    context <- NULL
  } else {
    assert_is(context, "context")
    context_id <- context$id
  }

  R6_rrq_controller$new(con, context_id, context)
}

R6_rrq_controller <- R6::R6Class(
  "rrq_controller",

  public = list(
    con = NULL,
    context_id = NULL,
    keys = NULL,

    ## Only present if we have a full context:
    context = NULL,
    db = NULL,

    initialize = function(con, context_id, context) {
      self$con <- con
      self$context_id <- context_id
      self$keys <- rrq_keys(context_id)
      if (!is.null(context)) {
        if (!is.environment(context$envir)) {
          stop("context must be loaded")
        }
        self$context <- context
        self$db <- context$db
        ## NOTE: This is required to save the running script.  This
        ## means that we can't easily spawn new workers without having
        ## set a context up _with_ queue support.
        write_rrq_worker(self$context)
        self$worker_config_save("localhost", copy_redis = TRUE,
                                overwrite = FALSE)
        ## This is used to create a hint as to who is using the queue.
        ## It's done as a list so will accumulate elements over time,
        ## but cap at the 10 most recent uses.
        push_controller_info(self$con, self$keys)
      }
    },

    ## This is super destructive; should we require this one to need
    ## the actual context object perhaps?
    destroy = function(delete = TRUE, type = "message") {
      rrq_clean(self$con, self$context_id, delete, type)
      ## render the controller useless:
      self$con <- NULL
      self$context_id <- NULL
      self$keys <- NULL
      self$context <- NULL
      self$db <- NULL
    },

    ## 0. Queuing
    enqueue = function(expr, envir = parent.frame(), key_complete = NULL) {
      self$enqueue_(substitute(expr), envir, key_complete)
    },
    enqueue_ = function(expr, envir = parent.frame(), key_complete = NULL) {
      dat <- context::prepare_expression(expr, envir, self$db)
      task_submit(self$con, self$keys, dat, key_complete)
    },
    ## TODO: These all need to have similar names, semantics,
    ## arguments as queuer if I will ever merge these and not go mad.
    ## Go through and check.
    ##
    ## NOTE: These require a loaded context, at least for now.  This
    ## is because we need to do some analysis on the bulk call against
    ## the context environment to set things up correctly.  It could
    ## probably be relaxed though.
    lapply = function(X, FUN, ..., DOTS = NULL,
                      envir = parent.frame(),
                      timeout = Inf, time_poll = 1, progress = NULL) {
      if (is.null(self$context)) {
        stop("lapply requires a loaded context")
      }
      rrq_lapply(self, X, FUN, ..., DOTS = NULL, envir = envir,
                 timeout = timeout, time_poll = time_poll,
                 progress = progress)
    },
    enqueue_bulk = function(X, FUN, ..., DOTS = NULL, do_call = TRUE,
                            envir = parent.frame(),
                            timeout = Inf, time_poll = 1, progress = NULL) {
      if (is.null(self$context)) {
        stop("enqueue_bulk requires a loaded context")
      }
      rrq_enqueue_bulk(self, X, FUN, ..., DOTS = DOTS, do_call = do_call,
                       envir = envir, timeout = timeout, time_poll = time_poll,
                       progress = progress)
    },

    ## 1. Tasks
    ##
    ## TODO: decide if the tasks/task split here is ideal.  It does
    ## seem reasonable at this point.  OTOH it conflicts with queuer
    ## where we use task_list, task_status etc.
    ##
    ## The big issue is things like tasks_result and tasks_wait which
    ## return vectors.  Another way of doing this is to have a
    ## `multiple = TRUE` argument which always returns a list.
    task_list = function() {
      as.character(self$con$HKEYS(self$keys$task_expr))
    },
    task_status = function(task_ids = NULL) {
      task_status(self$con, self$keys, task_ids)
    },
    task_overview = function(task_ids = NULL) {
      task_overview(self$con, self$keys, task_ids)
    },

    ## One result, as the object
    task_result = function(task_id) {
      assert_scalar_character(task_id)
      self$tasks_result(task_id)[[1L]]
    },
    ## zero, one or more tasks as a list
    tasks_result = function(task_ids) {
      task_results(self$con, self$keys, task_ids)
    },

    task_wait = function(task_id, timeout = Inf, time_poll = NULL,
                       progress = NULL, key_complete = NULL) {
      assert_scalar_character(task_id)
      self$tasks_wait(task_id, timeout, time_poll,
                      progress, key_complete)[[1L]]
    },
    tasks_wait = function(task_ids, timeout = Inf, time_poll = NULL,
                          progress = NULL, key_complete = NULL) {
      if (is.null(key_complete)) {
        collect_wait_n_poll(self$con, self$keys, task_ids,
                            timeout, time_poll, progress)
      } else {
        collect_wait_n(self$con, self$keys, task_ids, key_complete,
                       timeout, time_poll, progress)
      }
    },
    task_delete = function(task_ids, check = TRUE) {
      task_delete(self$con, self$keys, task_ids, check)
    },

    ## 2. Queue state
    ##
    ## TODO: the latter two (queue_submit, queue_unsubmit) were
    ## included only in the worker controller and are to control the
    ## context queue.  This needs harmonising!  I think that the first
    ## two functions get a flag indicating which queue we're looking
    ## at, and the latter two probably also need some sort of flag
    ## too.
    queue_length = function() {
      self$con$LLEN(self$keys$queue_rrq)
    },
    queue_list = function() {
      as.character(self$con$LRANGE(self$keys$queue_rrq, 0, -1))
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

    ## 3. Workers
    worker_len = function() {
      worker_len(self$con, self$keys)
    },
    worker_list = function() {
      worker_list(self$con, self$keys)
    },
    worker_list_exited = function() {
      worker_list_exited(self$con, self$keys)
    },
    worker_info = function(worker_ids = NULL) {
      worker_info(self$con, self$keys, worker_ids)
    },
    worker_status = function(worker_ids = NULL) {
      worker_status(self$con, self$keys, worker_ids)
    },
    worker_log_tail = function(worker_ids = NULL, n = 1) {
      worker_log_tail(self$con, self$keys, worker_ids, n)
    },
    worker_task_id = function(worker_ids = NULL) {
      worker_task_id(self$con, self$keys, worker_ids)
    },
    worker_delete_exited = function(worker_ids = NULL) {
      worker_delete_exited(self$con, self$keys, worker_ids)
    },
    worker_stop = function(worker_ids = NULL, type = "message",
                            timeout = 0, time_poll = 1, progress = NULL) {
      worker_stop(self$con, self$keys, worker_ids, type,
                   timeout, time_poll, progress)
    },
    ## This one is a bit unfortunately named, but should do for now.
    ## It only works if the worker has appropriately saved logging
    ## information.  Given the existance of things like
    ## worker_log_tail this should be renamed something like
    ## worker_text_log perhaps?
    worker_process_log = function(worker_id, parse = TRUE) {
      root <- self$context$root
      if (is.null(root)) {
        stop("To read the worker log, need access to context root")
      }
      assert_scalar(worker_id)
      context::task_log(worker_id, root, parse = parse)
    },
    worker_config_save = function(name, redis_host = NULL, redis_port = NULL,
                                  time_poll = NULL, timeout = NULL,
                                  log_path = NULL, heartbeat_period = NULL,
                                  copy_redis = FALSE, overwrite = TRUE) {
      root <- self$context$root
      if (is.null(root)) {
        stop("To save a worker config, need access to context root")
      }
      rrq_worker_config_save(root, self$con, name, redis_host, redis_port,
                             time_poll, timeout, log_path, heartbeat_period,
                             copy_redis, overwrite)
    },

    ## 4. Messaging
    message_send = function(command, args = NULL, worker_ids = NULL) {
      message_send(self$con, self$keys, command, args, worker_ids)
    },
    message_has_response = function(message_id, worker_ids = NULL,
                                    named = TRUE) {
      message_has_response(self$con, self$keys, message_id, worker_ids, named)
    },
    message_get_response = function(message_id, worker_ids = NULL, named = TRUE,
                                     delete = FALSE, timeout = 0,
                                     time_poll = 1, progress = NULL) {
      message_get_response(self$con, self$keys, message_id, worker_ids, named,
                           delete, timeout, time_poll, progress)
    },
    message_response_ids = function(worker_id) {
      message_response_ids(self$con, self$keys, worker_id)
    },
    ## All in one; this gets merged into the send function, just like
    ## the bulk submissing thing (TODO)
    message_send_and_wait = function(command, args = NULL, worker_ids = NULL,
                                     named = TRUE, delete = TRUE, timeout = 600,
                                     time_poll = 0.05, progress = NULL) {
      message_send_and_wait(self$con, self$keys, command, args, worker_ids,
                            named, delete, timeout, time_poll, progress)
    }
  ))

task_status <- function(con, keys, task_ids) {
  from_redis_hash(con, keys$task_status, task_ids, missing = TASK_MISSING)
}

task_overview <- function(con, keys, task_ids) {
  lvls <- c(TASK_PENDING, TASK_RUNNING, TASK_COMPLETE, TASK_ERROR)
  status <- task_status(con, keys, task_ids)
  lvls <- c(lvls, setdiff(unique(status), lvls))
  table(factor(status, lvls))
}

task_submit <- function(con, keys, dat, key_complete) {
  task_submit_n(con, keys, list(object_to_bin(dat)), key_complete)
}

task_delete <- function(con, keys, task_ids, check = TRUE) {
  if (check) {
    ## TODO: filter from the running list if not running, but be
    ## aware of race conditions. This is really only for doing
    ## things that have finished so could just check that the
    ## status is one of the finished ones.  Write a small lua
    ## script that can take the setdiff of these perhaps...
    st <- from_redis_hash(con, keys$task_status, task_ids,
                          missing = TASK_MISSING)
    if (any(st == "RUNNING")) {
      stop("Can't delete running tasks")
    }
  }
  con$HDEL(keys$task_expr,     task_ids)
  con$HDEL(keys$task_status,   task_ids)
  con$HDEL(keys$task_result,   task_ids)
  con$HDEL(keys$task_complete, task_ids)
  con$HDEL(keys$task_worker,   task_ids)
}

task_submit_n <- function(con, keys, dat, key_complete) {
  if (!(is.list(dat) && all(vlapply(dat, is.raw)))) {
    stop("dat must be a raw list")
  }
  n <- length(dat)
  task_ids <- ids::random_id(length(dat))

  con$pipeline(
    if (!is.null(key_complete)) {
      redis$HMSET(keys$task_complete, task_ids, rep_len(key_complete, n))
    },
    redis$HMSET(keys$task_expr, task_ids, dat),
    redis$HMSET(keys$task_status, task_ids, rep_len(TASK_PENDING, n)),
    redis$RPUSH(keys$queue_rrq, task_ids),
    redis$INCRBY(keys$task_count, length(task_ids))
  )

  task_ids
}

task_results <- function(con, keys, task_ids) {
  res <- from_redis_hash(con, keys$task_result, task_ids, identity, NULL)
  err <- lengths(res) == 0L
  if (any(err)) {
    stop("Missing some results")
  }
  lapply(res, bin_to_object)
}

worker_len <- function(con, keys) {
  con$SCARD(keys$worker_name)
}
worker_list <- function(con, keys) {
  worker_naturalsort(as.character(con$SMEMBERS(keys$worker_name)))
}

worker_list_exited <- function(con, keys) {
  setdiff(as.character(con$HKEYS(keys$worker_info)), worker_list(con, keys))
}

worker_status <- function(con, keys, worker_ids = NULL) {
  from_redis_hash(con, keys$worker_status, worker_ids)
}

worker_info <- function(con, keys, worker_ids = NULL) {
  from_redis_hash(con, keys$worker_info, worker_ids,
                  f = Vectorize(bin_to_object, SIMPLIFY = FALSE))
}

worker_log_tail <- function(con, keys, worker_ids = NULL, n = 1) {
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }
  tmp <- lapply(worker_ids, function(i) worker_log_tail_1(con, keys, i, n))
  if (length(tmp) > 0L) {
    n <- viapply(tmp, nrow)
    ret <- do.call("rbind", tmp, quote = TRUE)
    ret <- ret[order(ret$time, ret$worker_id), ]
    rownames(ret) <- NULL
    ret
  } else {
    data.frame(worker_id = character(0),
               time = character(0),
               command = character(0),
               message = character(0),
               stringsAsFactors = FALSE)
  }
}

worker_log_tail_1 <- function(con, keys, worker_id, n = 1) {
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
  data.frame(worker_id, time, command, message, stringsAsFactors = FALSE)
}

worker_task_id <- function(con, keys, worker_id) {
  from_redis_hash(con, keys$worker_task, worker_id)
}

worker_delete_exited <- function(con, keys, worker_ids = NULL) {
  ## This only includes things that have been processed and had task
  ## orphaning completed.
  if (is.null(worker_ids)) {
    worker_ids <- worker_list_exited(con, keys)
  } else {
    extra <- setdiff(worker_ids, worker_list_exited(con, keys))
    if (length(extra)) {
      stop(sprintf(
        ## TODO: this function does not exist!  It's in rrqueue and I
        ## think depends on heartbeat.
        "Workers %s may not have exited;\n\trun worker_identify_lost first",
        paste(extra, collapse = ", ")))
    }
  }
  if (length(worker_ids) > 0L) {
    con$HDEL(keys$worker_name,   worker_ids)
    con$HDEL(keys$worker_status, worker_ids)
    con$HDEL(keys$worker_task,   worker_ids)
    con$HDEL(keys$worker_info,   worker_ids)
    con$DEL(c(rrq_key_worker_log(keys$queue_name, worker_ids),
              rrq_key_worker_message(keys$queue_name, worker_ids),
              rrq_key_worker_response(keys$queue_name, worker_ids)))
  }
  worker_ids
}

worker_stop <- function(con, keys, worker_ids = NULL, type = "message",
                         timeout = 0, time_poll = 1, progress = NULL) {
  type <- match.arg(type, c("message", "kill", "kill_local"))
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }
  if (length(worker_ids) == 0L) {
    return(invisible(worker_ids))
  }

  if (type == "message") {
    ## First, we send a message saying 'STOP'
    message_id <- message_send(con, keys, "STOP", worker_ids = worker_ids)
    ## ## TODO: This needs RedisHeartbeat support
    ##
    ## if (interrupt) {
    ##   is_busy <- worker_status(con, keys, worker_ids) == WORKER_BUSY
    ##   if (any(is_busy)) {
    ##     queue_send_signal(con, keys, tools::SIGINT, worker_ids[is_busy])
    ##   }
    ## }
    if (timeout > 0L) {
      ok <- try(message_get_response(con, keys, message_id, worker_ids,
                                     delete = FALSE, timeout = timeout,
                                     time_poll = time_poll,
                                     progress = progress),
                silent = TRUE)
      ## if (inherits(ok, "try-error")) {
      ##   done <- message_has_response(con, keys, message_id, worker_ids)
      ##   worker_stop(con, keys, worker_ids[!done], "kill")
      ## }
    }
    ## TODO: This requires RedisHeartbeat support
  } else if (type == "kill") {
    ## queue_send_signal(con, keys, tools::SIGTERM, worker_ids)
    stop("Needs redis heartbeat support")
  } else {
    ## kill_local
    w_info <- worker_info(con, keys, worker_ids)
    w_local <- vcapply(w_info, "[[", "hostname") == hostname()
    if (!all(w_local)) {
      stop("Not all workers are local: ",
           paste(worker_ids[!w_local], collapse = ", "))
    }
    tools::pskill(vnapply(w_info, "[[", "pid"), tools::SIGTERM)
  }
  invisible(worker_ids)
}

## There are a whole bunch of collection functions here.
##
## * collect_wait_n:      sits on a special key, so is quite responsive
## * collect_wait_n_poll: actively polls the hashes
collect_wait_n <- function(con, keys, task_ids, key_complete,
                           timeout, time_poll, progress) {
  time_poll <- time_poll %||% 1
  assert_integer_like(time_poll)
  if (time_poll < 1L) {
    warning(
      "time_poll cannot be less than 1 with this function; increasing to 1")
    time_poll <- 1L
  }

  status <- from_redis_hash(con, keys$task_status, task_ids)

  done <- status == TASK_COMPLETE | status == TASK_ERROR
  if (all(done)) {
    con$DEL(key_complete)
  } else {
    p <- queuer::progress_timeout(total = length(status),
                                  show = progress, timeout = timeout)
    while (!all(done)) {
      tmp <- con$BLPOP(key_complete, time_poll)
      if (is.null(tmp)) {
        if (p(0)) {
          p(clear = TRUE)
          stop(sprintf("Exceeded maximum time (%d / %d tasks pending)",
                       sum(!done), length(task_ids)))
        }
      } else {
        p(1)
        id <- tmp[[2L]]
        done[[id]] <- TRUE
      }
    }
  }

  task_results(con, keys, task_ids)
}

collect_wait_n_poll <- function(con, keys, task_ids,
                                timeout, time_poll, progress) {
  time_poll <- time_poll %||% 0.1
  status <- from_redis_hash(con, keys$task_status, task_ids)
  done <- status == TASK_COMPLETE | status == TASK_ERROR
  if (all(done)) {
    ## pass
  } else if (timeout == 0) {
    stop("Tasks not yet completed; can't be immediately returned")
  } else {
    p <- queuer::progress_timeout(length(task_ids),
                                  show = progress, timeout = timeout)
    remaining <- task_ids[!done]
    while (length(remaining) > 0L) {
      exists <- viapply(remaining, con$HEXISTS, key = keys$task_result) == 1L
      if (any(exists)) {
        i <- remaining[exists]
        p(length(i))
        remaining <- remaining[!exists]
      } else {
        if (p(0)) {
          p(clear = TRUE)
          stop(sprintf("Exceeded maximum time (%d / %d tasks pending)",
                       length(remaining), length(task_ids)))
        }
        Sys.sleep(time_poll)
      }
    }
  }
  task_results(con, keys, task_ids)
}

controller_info <- function() {
  list(hostname = hostname(),
       pid = process_id(),
       username = username(),
       time = Sys.time())
}

push_controller_info <- function(con, keys, max_n = 10) {
  n <- con$RPUSH(keys$controller, object_to_bin(controller_info()))
  if (n > max_n) {
    con$LTRIM(keys$controller, -max_n, -1)
  }
}

worker_naturalsort <- function(x) {
  re <- "^(.*)_(\\d+)$"
  root <- sub(re, "\\1", x)
  idx <- as.integer(sub(re, "\\2", x))
  x[order(root, idx)]
}
