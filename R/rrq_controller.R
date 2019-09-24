##' A queue controller.  Use this to interact with a queue/cluster.
##'
##' @title rrq queue controller
##'
##' @param name An identifier for the queue.  This will prefix all
##'   keys in redis, so a prefix might be useful here depending on
##'   your use case (e.g. \code{rrq:<user>:<id>})
##'
##' @param con A redis connection
##'
##' @export
rrq_controller <- function(name, con = redux::hiredis()) {
  assert_scalar_character(name)
  assert_is(con, "redis_api")
  R6_rrq_controller$new(con, name)
}

R6_rrq_controller <- R6::R6Class(
  "rrq_controller",

  public = list(
    con = NULL,
    name = NULL,
    keys = NULL,
    db = NULL,

    initialize = function(con, name) {
      self$con <- con
      self$keys <- rrq_keys(name)
      self$worker_config_save("localhost", overwrite = FALSE)
      ## it is possible that it will be userful to have a storr db
      ## here in the object to store things like locals.  In which
      ## case we'll want to do
      ##
      self$db <- redux::storr_redis_api(self$keys$db_prefix, self$con)
      ##
      ## or similar.
      info <- object_to_bin(controller_info())
      rpush_max_length(self$con, self$keys$controller, info, 10)
    },

    ## This is super destructive
    destroy = function(delete = TRUE, type = "message",
                       worker_stop_timeout = 0) {
      if (!is.null(self$con)) {
        rrq_clean(self$con, self$name, delete, type, worker_stop_timeout)
        ## render the controller useless:
        self$con <- NULL
        self$name <- NULL
        self$keys <- NULL
      }
    },

    envir = function(create, notify = TRUE) {
      assert_is(create, "function")
      self$con$SET(self$keys$envir, object_to_bin(create))
      if (notify) {
        self$message_send("REFRESH")
      }
    },

    ## 0. Queuing
    enqueue = function(expr, envir = parent.frame(), key_complete = NULL) {
      self$enqueue_(substitute(expr), envir, key_complete)
    },

    enqueue_ = function(expr, envir = parent.frame(), key_complete = NULL) {
      dat <- context::prepare_expression(expr, envir, self$db)
      task_submit(self$con, self$keys, dat, key_complete)
    },

    call = function(FUN, ..., envir = parent.frame(), key_complete = NULL) {
      rrq_enqueue_bulk_submit(self, list(list(...)), FUN,
                              envir = envir, do_call = TRUE,
                              key_complete = key_complete)$task_ids
    },

    lapply = function(X, FUN, ..., DOTS = NULL,
                      envir = parent.frame(),
                      timeout = Inf, time_poll = 1, progress = NULL) {
      rrq_lapply(self, X, FUN, ..., DOTS = NULL, envir = envir,
                 timeout = timeout, time_poll = time_poll,
                 progress = progress)
    },

    enqueue_bulk = function(X, FUN, ..., DOTS = NULL, do_call = TRUE,
                            envir = parent.frame(),
                            timeout = Inf, time_poll = 1, progress = NULL) {
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

    task_position = function(task_ids, missing = 0L) {
      task_position(self$con, self$keys, task_ids, missing)
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
      tasks_wait(self$con, self$keys, task_ids, timeout, time_poll, progress,
                 key_complete)
    },

    task_delete = function(task_ids, check = TRUE) {
      task_delete(self$con, self$keys, task_ids, check)
    },

    ## 2. Fast queue
    queue_length = function() {
      self$con$LLEN(self$keys$queue_rrq)
    },

    queue_list = function() {
      list_to_character(self$con$LRANGE(self$keys$queue_rrq, 0, -1))
    },

    ## 4. Workers
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
      stop("not implemented")
      ## This is going to require access to the same physical storage
      ## on workers as on the controller and working that out will be
      ## a trick.
      root <- self$context$root
      if (is.null(root)) {
        stop("To read the worker log, need access to context root")
      }
      assert_scalar(worker_id)
      context::task_log(worker_id, root, parse = parse)
    },

    worker_config_save = function(name, time_poll = NULL, timeout = NULL,
                                  log_path = NULL, heartbeat_period = NULL,
                                  overwrite = TRUE) {
      worker_config_save(self$con, self$keys, name, time_poll, timeout,
                         log_path, heartbeat_period, overwrite)
    },

    worker_load = function(worker_ids = NULL, ...) {
      worker_load(self$con, self$keys, worker_ids, ...)
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
  as.list(table(factor(status, lvls)))
}

## NOTE: This is not crazy efficient; we pull the entire list down
## which is not ideal.  However, in practice it seems fairly fast.
## But one should be careful to adjust the polling interval of
## something usnig this not to flood the server with excessive load.
##
## A better way would possibly be to use a LUA script; especially for
## the case where there is a single job that'd be fairly easy to do.
task_position <- function(con, keys, task_ids, missing) {
  queue <- vcapply(con$LRANGE(keys$queue_rrq, 0, -1L), identity)
  match(task_ids, queue, missing)
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
  invisible()
}

task_submit_n <- function(con, keys, dat, key_complete) {
  n <- length(dat)
  task_ids <- ids::random_id(n)

  con$pipeline(
    if (!is.null(key_complete)) {
      redis$HMSET(keys$task_complete, task_ids, rep_len(key_complete, n))
    },
    redis$HMSET(keys$task_expr, task_ids, dat),
    redis$HMSET(keys$task_status, task_ids, rep_len(TASK_PENDING, n)),
    redis$RPUSH(keys$queue, task_ids))

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
                  f = Vectorize(bin_to_object_safe, SIMPLIFY = FALSE))
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
    data_frame(worker_id = character(0),
               time = numeric(0),
               command = character(0),
               message = character(0))
  }
}

worker_log_tail_1 <- function(con, keys, worker_id, n = 1) {
  ## More intuitive `n` behaviour for "print all entries"; n of Inf
  if (identical(n, Inf)) {
    n <- 0
  }
  log_key <- rrq_key_worker_log(keys$queue_name, worker_id)
  log <- as.character(con$LRANGE(log_key, -n, -1))
  worker_log_parse(log, worker_id)
}


worker_log_parse <- function(log, worker_id) {
  re <- "^([0-9.]+) ([^ ]+) ?(.*)$"
  if (!all(grepl(re, log))) {
    stop("Corrupt log")
  }
  time <- as.numeric(sub(re, "\\1", log))
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
  exited <- worker_list_exited(con, keys)
  if (is.null(worker_ids)) {
    worker_ids <- exited
  }
  extra <- setdiff(worker_ids, exited)
  if (length(extra)) {
    ## TODO: this whole thing can be improved because we might want to
    ## inform the user if the workers are not known.
    stop(sprintf("Workers %s may not have exited or may not exist",
                 paste(extra, collapse = ", ")))
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
    message_id <- message_send(con, keys, "STOP", worker_ids = worker_ids)
    if (timeout > 0L) {
      message_get_response(con, keys, message_id, worker_ids,
                           delete = FALSE, timeout = timeout,
                           time_poll = time_poll,
                           progress = progress)
    }
  } else if (type == "kill") {
    info <- worker_info(con, keys, worker_ids)
    heartbeat_key <- vcapply(info, function(x)
      x$heartbeat_key %||% NA_character_)
    if (any(is.na(heartbeat_key))) {
      stop("Worker does not support heatbeat - can't kill with signal: ",
           paste(worker_ids[is.na(heartbeat_key)], collapse = ", "))
    }
    for (key in heartbeat_key) {
      heartbeatr::heartbeat_send_signal(con, key, tools::SIGTERM)
    }
  } else { # kill_local
    info <- worker_info(con, keys, worker_ids)
    is_local <- vcapply(info, "[[", "hostname") == hostname()
    if (!all(is_local)) {
      stop("Not all workers are local: ",
           paste(worker_ids[!is_local], collapse = ", "))
    }
    tools::pskill(vnapply(info, "[[", "pid"), tools::SIGTERM)
  }
  invisible(worker_ids)
}


controller_info <- function() {
  list(hostname = hostname(),
       pid = process_id(),
       username = username(),
       time = Sys.time())
}

worker_naturalsort <- function(x) {
  re <- "^(.*)_(\\d+)$"
  root <- sub(re, "\\1", x)
  i <- grepl(re, x)
  idx <- numeric(length(x))
  idx[i] <- as.integer(sub(re, "\\2", x[i]))
  x[order(root, idx)]
}

## This is very much a beginning here; it might be nicer to be able to
## do this for a given time interval as well as computing a rolling
## average (to plot, for example).  But the concept is here now and we
## can build off of it.
worker_load <- function(con, keys, worker_ids) {
  logs <- worker_log_tail(con, keys, worker_ids, Inf)
  logs <- logs[order(logs$time), ]

  logs$worker <- 0
  logs$worker[logs$command == "ALIVE"] <- 1
  logs$worker[logs$command == "STOP"] <- -1
  logs$worker_cum <- cumsum(logs$worker)

  logs$task <- 0
  logs$task[logs$command == "TASK_START"] <- 1
  logs$task[logs$command == "TASK_COMPLETE"] <- -1
  logs$task_cum <- cumsum(logs$task)

  logs$dt <- c(diff(logs$time), 0)
  logs$ago <- logs$time[nrow(logs)] - logs$time

  class(logs) <- c("worker_load", class(logs))
  logs
}

##' @export
mean.worker_load <- function(x, time = c(1, 5, 15, Inf), ...) {
  ## make this slightly nicer to work with:
  x$dt <- x$dt * 1000
  x$ago <- x$ago * 1000

  f <- function(t) {
    i <- which(x$ago < t)[[1L]] - 1L
    y <- x[i:nrow(x), ]
    if (i > 0) {
      dt <- y$ago[[1L]] - t
      y$dt[[1L]] <- y$dt[[1L]] - dt
      y$time[[1L]] <- y$time[[1L]] - dt
      y$ago[[1L]] <- t
    }
    c(used = sum(y$task_cum * y$dt) / y$ago[[1L]],
      available = sum(y$worker_cum * y$dt) / y$ago[[1L]])
  }
  res <- vapply(time, f, numeric(2))
  colnames(res) <- as.character(time)
  res
}


tasks_wait <- function(con, keys, task_ids, timeout, time_poll = NULL,
                       progress = NULL, key_complete = NULL) {
  if (is.null(key_complete)) {
    done <- rep(FALSE, length(task_ids))
    fetch <- function() {
      done[!done] <<- hash_exists(con, keys$task_result, task_ids[!done], TRUE)
      done
    }
    general_poll(fetch, time_poll %||% 0.05, timeout, "tasks", TRUE, progress)
  } else {
    time_poll <- time_poll %||% 1
    assert_integer_like(time_poll)
    if (time_poll < 1L) {
      stop("time_poll cannot be less than 1 if using key_complete")
    }
    done <- set_names(
      hash_exists(con, keys$task_result, task_ids, TRUE),
      task_ids)

    fetch <- function() {
      tmp <- con$BLPOP(key_complete, time_poll)
      if (!is.null(tmp)) {
        done[[tmp[[2L]]]] <<- TRUE
      }
      done
    }
    general_poll(fetch, 0, timeout, "tasks", TRUE, progress)
  }

  task_results(con, keys, task_ids)
}


rrq_db <- function(con, keys) {
  redux::storr_redis_api(keys$db_prefix, con)
}
