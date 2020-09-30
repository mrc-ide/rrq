##' A queue controller.  Use this to interact with a queue/cluster.
##'
##' @title rrq queue controller
##'
##' @param queue_id An identifier for the queue.  This will prefix all
##'   keys in redis, so a prefix might be useful here depending on
##'   your use case (e.g. \code{rrq:<user>:<id>})
##'
##' @param con A redis connection
##'
##' @section Task lifecycle:
##'
##' * A task is queued with `$enqueue()`, at which point it becomes `PENDING`
##' * Once a worker selects the task to run, it becomes `RUNNING`
##' * If the task completes successfully without error it becomes `COMPLETE`
##' * If the task throws an error, it becomes `ERROR`
##' * If the worker was interrupted (e.g., by a message) the task
##'   becomes `INTERRUPTED`
##' * If the worker crashes, possibly due to the task, *and* the worker runs
##'   a heartbeat the task becomes `ORPHAN`
##' * The status of an unknown task is `MISSING`
##'
##' @section Worker lifecycle:
##'
##' * A worker appears and is `IDLE`
##' * When running a task it is `BUSY`
##' * If it recieves a `PAUSE` message it becomes `PAUSED` until it
##'   recieves a `RESUME` message
##' * If it exits cleanly (e.g., via a `STOP` message or a timeout) it
##'   becomes `EXITED`
##' * If it crashes and was running a heartbeat, it becomes `LOST`
##'
##' @export
rrq_controller <- function(queue_id, con = redux::hiredis()) {
  assert_scalar_character(queue_id)
  assert_is(con, "redis_api")
  R6_rrq_controller$new(con, queue_id)
}

R6_rrq_controller <- R6::R6Class(
  "rrq_controller",
  cloneable = FALSE,

  public = list(
    con = NULL,
    queue_id = NULL,
    keys = NULL,
    db = NULL,

    initialize = function(con, queue_id) {
      self$con <- con
      self$queue_id <- queue_id
      self$keys <- rrq_keys(self$queue_id)
      self$worker_config_save("localhost", overwrite = FALSE)
      self$db <- rrq_db(self$con, self$keys)
      info <- object_to_bin(controller_info())
      rpush_max_length(self$con, self$keys$controller, info, 10)
    },

    ##' @description Entirely destroy a queue, by deleting all keys
    ##' associated with it from the Redis database. This is a very
    ##' destructive action and cannot be undone.
    ##'
    ##' @param delete Either `TRUE` (the default) indicating that the
    ##'   keys should be immediately deleted. Alternatively, provide an
    ##'   integer value and the keys will instead be marked for future
    ##'   deletion by "expiring" after this many seconds, using Redis'
    ##'   `EXPIRE` command.
    ##'
    ##' @param worker_stop_type Passed to `$worker_stop`; Can be one of
    ##'   "message", "kill" or "kill_local". The "kill" method requires that
    ##'   the workers are using a heartbeat, and "kill_local" requires that
    ##'   the workers are on the same machine as the controller. However,
    ##'   these may be faster to stop workers than "message", which will
    ##'   wait until any task is finished.
    ##'
    ##' @param worker_stop_timeout A timeout to pass to the worker if
    ##'   using `type = "message"`
    destroy = function(delete = TRUE, worker_stop_type = "message",
                       worker_stop_timeout = 0) {
      if (!is.null(self$con)) {
        rrq_clean(self$con, self$queue_id, delete, type,
                  worker_stop_timeout)
        ## render the controller useless:
        self$con <- NULL
        self$queue_id <- NULL
        self$keys <- NULL
      }
    },

    ##' @description Register a function to create an environment when
    ##'   creating a worker. When a worker starts, they will run this
    ##'   function.
    ##'
    ##' @param create A function that will create an environment. It will
    ##'   be called with no parameters, in a fresh R session.
    ##'
    ##' @param notify Boolean, indicating if we should send a `REFRESH`
    ##'   message to all workers to update their environment.
    envir = function(create, notify = TRUE) {
      if (is.null(create)) {
        self$con$DEL(self$keys$envir)
      } else {
        assert_is(create, "function")
        self$con$SET(self$keys$envir, object_to_bin(create))
      }
      if (notify) {
        self$message_send("REFRESH")
      }
    },

    ##' @description Queue an expression
    ##'
    ##' @param expr Any R expression, unevaluated
    ##'
    ##' @param envir The environment that you would run this expression in
    ##'   locally. This will be used to copy across any dependent variables.
    ##'   For example, if your expression is `sum(1 + a)`, we will also send
    ##'   the value of `a` to the worker along with the expression.
    ##
    ##' @param key_complete The name of a Redis key to write to once the
    ##'   task is complete. You can use this with `$task_wait` to efficiently
    ##'   wait for the task to complete (i.e., without using a busy loop).
    enqueue = function(expr, envir = parent.frame(), key_complete = NULL) {
      self$enqueue_(substitute(expr), envir, key_complete)
    },

    ##' @description Queue an expression
    ##'
    ##' @param expr Any R expression, quoted; use this to use `$enqueue`
    ##' in a programmatic context where you want to construct expressions
    ##' directly (e.g., `bquote(log(.(x)), list(x = 10))`
    ##'
    ##' @param envir The environment that you would run this expression in
    ##'   locally. This will be used to copy across any dependent variables.
    ##'   For example, if your expression is `sum(1 + a)`, we will also send
    ##'   the value of `a` to the worker along with the expression.
    ##
    ##' @param key_complete The name of a Redis key to write to once the
    ##'   task is complete. You can use this in conjunction with something
    ##'   like `BLPOP` to wait until a task is complete without a busy (sleep)
    ##'   loop.
    enqueue_ = function(expr, envir = parent.frame(), key_complete = NULL) {
      dat <- expression_prepare(expr, envir, NULL, self$db)
      task_submit(self$con, self$keys, dat, key_complete)
    },


    ##' @description List ids of all tasks known to this rrq controller
    task_list = function() {
      as.character(self$con$HKEYS(self$keys$task_expr))
    },

    ##' @description Return a character vector of task statuses. The name
    ##' of each element corresponds to a task id, and the value will the
    ##' one of the possible statuses ("PENDING", "COMPLETE", etc).
    ##'
    ##' @param task_ids Optional character vector of task ids for which you
    ##' would like statuses. If not given (or `NULL`) then the status of
    ##' all task ids known to this rrq controller is returned.
    task_status = function(task_ids = NULL) {
      task_status(self$con, self$keys, task_ids)
    },

    ##' @description Retrieve task progress, if set. This will be `NULL`
    ##'   if progress has never been registered, otherwise whatever value
    ##'   was set - can be an arbitrary R object.
    ##'
    ##' @param task_id A single task id for which the progress is wanted.
    task_progress = function(task_id) {
      task_progress(self$con, self$keys, task_id)
    },

    ##' @description Provide a high level overview of task statuses
    ##' for a set of task ids, being the count in major categories of
    ##' `PENDING`, `RUNNING`, `COMPLETE` and `ERROR`.
    task_overview = function(task_ids = NULL) {
      task_overview(self$con, self$keys, task_ids)
    },

    ##' @description Find the position of one or more tasks in the queue.
    ##'
    ##' @param task_ids Character vector of tasks to find the position for.
    ##'
    ##' @param missing Value to return if the task is not found in the queue.
    ##'   A task will take value `missing` if it is running, complete,
    ##'   errored etc and a positive integer if it is in the queue,
    ##'   indicating its position (with 1) being the next task to run.
    task_position = function(task_ids, missing = 0L) {
      task_position(self$con, self$keys, task_ids, missing)
    },

    ##' @description Get the result for a single task (see `$tasks_result`
    ##'   for a method for efficiently getting multiple results at once).
    ##'   Returns the value of running the task if it is complete, and an
    ##'   error otherwise.
    ##'
    ##' @param task_id The single id for which the result is wanted.
    task_result = function(task_id) {
      assert_scalar_character(task_id)
      self$tasks_result(task_id)[[1L]]
    },

    ##' @description Get the results of a group of tasks, returning them as a
    ##' list.
    ##'
    ##' @param task_ids A vector of task ids for which the task result
    ##' is wanted.
    tasks_result = function(task_ids) {
      task_results(self$con, self$keys, task_ids)
    },

    ##' @description Poll for a task to complete, returning the result
    ##' when completed. If the task has already completed this is
    ##' roughly equivalent to `task_result`. See `$tasks_wait` for an
    ##' efficient way of doing this for a group of tasks.
    ##'
    ##' @param task_id The single id that we will wait for
    ##'
    ##' @param timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if the task has not completed.
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion. The default and behaviour depend on `key_complete` -
    ##'   see the details section.
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param key_complete Optional key used when `enqueing` the tasks
    ##'   that will be written to on completion.
    ##'
    ##' @details The polling behaviour depends on `key_complete`. If
    ##' `key_complete` is `NULL` then we use a busy-loop, sleeping for
    ##' `time_poll` seconds between polls of Redis. As such, the smaller
    ##' the `time_poll` the faster you will get your result, as it may
    ##' be delayed by up to `time_poll`, but the heavier the network and
    ##' Redis load (the default is 0.05s). Alternatively, if
    ##' `key_complete` is given then your task will be returned as soon
    ##' as it is written into Redis, and `time_poll` is the time between
    ##' subsequent calls to the Redis function that enables
    ##' this. Shorter values of `time_poll` then make R more responsive
    ##' to being interrupted and longer values reduce network load (the
    ##' default is 1s)
    task_wait = function(task_id, timeout = Inf, time_poll = NULL,
                       progress = NULL, key_complete = NULL) {
      assert_scalar_character(task_id)
      self$tasks_wait(task_id, timeout, time_poll,
                      progress, key_complete)[[1L]]
    },

    ##' @description Poll for a group of tasks to complete, returning the
    ##' result as list when completed. If the tasks have already completed
    ##' this is roughly equivalent to `tasks_result`.
    ##'
    ##' @param task_ids A vector of task ids to poll for
    ##'
    ##' @param timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if the task has not completed.
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion. The default and behaviour depend on `key_complete` -
    ##'   see the details section of `$task_wait`
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param key_complete Optional key used when `enqueing` the tasks
    ##'   that will be written to on completion. If used, then all tasks
    ##'   must share the same completion key.
    tasks_wait = function(task_ids, timeout = Inf, time_poll = NULL,
                          progress = NULL, key_complete = NULL) {
      tasks_wait(self$con, self$keys, task_ids, timeout, time_poll, progress,
                 key_complete)
    },

    ##' @description Delete one or more tasks
    ##'
    ##' @param task_ids Vector of task ids to delete
    ##'
    ##' @param check Logical indicating if we should check that the tasks
    ##'   are not running. Deleting running tasks is unlikely to result in
    ##'   desirable behaviour.
    task_delete = function(task_ids, check = TRUE) {
      task_delete(self$con, self$keys, task_ids, check)
    },

    ##' @description Cancel a single task. If the task is `PENDING` it
    ##' will be deleted. If `RUNNING` then the worker will be
    ##' interrupted if it supports this.
    ##'
    ##' @param task_id Id of the task to cancel
    task_cancel = function(task_id) {
      task_cancel(self$con, self$keys, task_id)
    },

    ##' @description Fetch internal data about a task from Redis
    ##' (expert use only).
    ##'
    ##' @param task_id The id of the task
    task_data = function(task_id) {
      task_data(self$con, self$keys, self$db, task_id)
    },

    ##' @description Returns the number of tasks in the queue (waiting for
    ##' an available worker).
    queue_length = function() {
      self$con$LLEN(self$keys$queue)
    },

    ##' @description Returns the keys in the task queue.
    queue_list = function() {
      list_to_character(self$con$LRANGE(self$keys$queue, 0, -1))
    },

    ##' @description Remove task ids from a queue.
    ##'
    ##' @param task_ids Task ids to remove
    queue_remove = function(task_ids) {
      ## NOTE: uses a pipeline to avoid a race condition - nothing may
      ## interere with the queue between the LRANGE and the DEL or we
      ## might lose tasks or double-queue them. If a job is queued
      ## between the DEL and the RPUSH the newly submitted job gets
      ## bounced ahead in the queue, which seems tolerable but might not
      ## always be ideal.  To solve this we should use a lua script.
      queue_remove(self$con, self$keys, task_ids)
    },

    ##' @description Returns the number of active workers
    worker_len = function() {
      worker_len(self$con, self$keys)
    },

    ##' @description Returns the ids of active workers
    worker_list = function() {
      worker_list(self$con, self$keys)
    },

    ##' @description Returns the ids of workers known to have exited
    worker_list_exited = function() {
      worker_list_exited(self$con, self$keys)
    },

    ##' @description Returns a list of information about active
    ##' workers (or exited workers if `worker_ids` includes them).
    ##
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' all active workers are used.
    worker_info = function(worker_ids = NULL) {
      worker_info(self$con, self$keys, worker_ids)
    },

    ##' @description Returns a character vector of current worker statuses
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' all active workers are used.
    worker_status = function(worker_ids = NULL) {
      worker_status(self$con, self$keys, worker_ids)
    },

    ##' @description Returns the last (few) elements in the worker
    ##' log. The log will be returned as a [data.frame] of entries
    ##' `worker_id` (the worker id), `time` (the time in Redis when the
    ##' event happened; see [redux::redis_time] to convert this to an R
    ##' time), `command` (the worker command) and `message` (the message
    ##' corresponding to that command).
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' all active workers are used.
    ##'
    ##' @param n Number of elements to select, the default being the single
    ##' last entry. Use `Inf` or `0` to indicate that you want all log entries
    worker_log_tail = function(worker_ids = NULL, n = 1) {
      worker_log_tail(self$con, self$keys, worker_ids, n)
    },

    ##' @description Returns the task id that each worker is working on,
    ##' if any.
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' all active workers are used.
    worker_task_id = function(worker_ids = NULL) {
      worker_task_id(self$con, self$keys, worker_ids)
    },

    ##' @description Cleans up workers known to have exited
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' rrq looks for exited workers.
    worker_delete_exited = function(worker_ids = NULL) {
      worker_delete_exited(self$con, self$keys, worker_ids)
    },

    worker_stop = function(worker_ids = NULL, type = "message",
                            timeout = 0, time_poll = 1, progress = NULL) {
      worker_stop(self$con, self$keys, worker_ids, type,
                   timeout, time_poll, progress)
    },

    ## Detects exited workers through a lapsed heartbeat
    worker_detect_exited = function() {
      worker_detect_exited(self)
    },

    ## This is likely only to work with workers started by
    ## worker_spawn, and will basically only work when we're on the
    ## same physical storage.
    worker_process_log = function(worker_id, parse = TRUE) {
      assert_scalar(worker_id)
      path <- self$con$HGET(self$keys$worker_process, worker_id)
      if (is.null(path)) {
        stop("Process log not enabled for this worker")
      }
      readLines(path)
    },

    worker_config_save = function(name, time_poll = NULL, timeout = NULL,
                                  heartbeat_period = NULL, verbose = NULL,
                                  overwrite = TRUE) {
      worker_config_save(self$con, self$keys, name, time_poll, timeout,
                         heartbeat_period, verbose, overwrite)
    },

    worker_config_list = function() {
      list_to_character(self$con$HKEYS(self$keys$worker_config))
    },

    worker_config_read = function(name) {
      worker_config_read(self$con, self$keys, name)
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


task_progress <- function(con, keys, task_id) {
  assert_scalar_character(task_id)
  ret <- con$HGET(keys$task_progress, task_id)
  if (!is.null(ret)) {
    ret <- bin_to_object(ret)
  }
  ret
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
  queue <- vcapply(con$LRANGE(keys$queue, 0, -1L), identity)
  match(task_ids, queue, missing)
}

task_submit <- function(con, keys, dat, key_complete) {
  task_submit_n(con, keys, list(object_to_bin(dat)), key_complete)
}

task_delete <- function(con, keys, task_ids, check = TRUE) {
  if (check) {
    st <- from_redis_hash(con, keys$task_status, task_ids,
                          missing = TASK_MISSING)
    if (any(st == TASK_RUNNING)) {
      stop("Can't delete running tasks")
    }
  }
  con$pipeline(
    redis$HDEL(keys$task_expr,     task_ids),
    redis$HDEL(keys$task_status,   task_ids),
    redis$HDEL(keys$task_result,   task_ids),
    redis$HDEL(keys$task_complete, task_ids),
    redis$HDEL(keys$task_progress, task_ids),
    redis$HDEL(keys$task_worker,   task_ids))
  queue_remove(con, keys, task_ids)
  invisible()
}

## This is tricky because we need to ensure that the workers involved
## do not pick up any extra work, so we'll pause them, then check.  We
## need to assume that due to the possibility of a race condition that
## at any point between adjacent redis operations the state of the
## worker might change (it could finish the task for example).
##
## The steps are
##
## 1. identify the worker that the task is being run on and its state
##    - confirming that the task is actually running
##
## 2. pause the worker so that it won't pick up any extra tasks (that
##    stops us accidentally interrupting the next job in the queue if
##    the worker finishes immediately after step 1
##
## 3. confirm that the worker is actually busy and that it is working
##    on the expected task
##
## 4. find the heartbeat key, confirming that worker actually supports
##    heartbeat
##
## 5. send an interrupt signal
##
## 6. resume the worker so that it can start with new tasks
task_cancel <- function(con, keys, task_id) {
  dat <- con$pipeline(
    worker = redis$HGET(keys$task_worker, task_id),
    status = redis$HGET(keys$task_status, task_id))

  if (is.null(dat$status)) {
    dat$status <- TASK_MISSING
  }

  if (dat$status == TASK_PENDING) {
    task_delete(con, keys, task_id, FALSE)
    dat <- con$pipeline(
      worker = redis$HGET(keys$task_worker, task_id),
      status = redis$HGET(keys$task_status, task_id))
    if (is.null(dat$status)) {
      return(TRUE)
    }
  }

  if (dat$status != TASK_RUNNING) {
    stop(sprintf("Task %s is not running (%s)", task_id, dat$status))
  }

  worker_id <- dat$worker
  message_send(con, keys, "PAUSE", worker_ids = worker_id)
  on.exit(message_send(con, keys, "RESUME", worker_ids = worker_id))
  dat <- con$pipeline(
    task = redis$HGET(keys$worker_task, worker_id),
    status = redis$HGET(keys$worker_status, worker_id),
    info = redis$HGET(keys$worker_info, worker_id))

  if (dat$status != WORKER_BUSY || dat$task != task_id) {
    stop("Task finished during check")
  }

  heartbeat_key <- bin_to_object_safe(dat$info)$heartbeat_key
  if (is.null(heartbeat_key)) {
    stop("Worker does not have heartbeat enabled")
  }

  heartbeatr::heartbeat_send_signal(con, heartbeat_key, tools::SIGINT)
  invisible(TRUE)
}


task_data <- function(con, keys, db, task_id) {
  expr <- con$HGET(keys$task_expr, task_id)
  if (is.null(expr)) {
    stop(sprintf("Task '%s' not found", task_id))
  }
  task <- bin_to_object(expr)
  data <- as.list(expression_restore_locals(task, emptyenv(), db))
  task$objects <- data[names(task$objects)]
  task
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
  log_key <- rrq_key_worker_log(keys$queue_id, worker_id)
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
    con$DEL(c(rrq_key_worker_log(keys$queue_id, worker_ids),
              rrq_key_worker_message(keys$queue_id, worker_ids),
              rrq_key_worker_response(keys$queue_id, worker_ids)))
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


queue_remove <- function(con, keys, task_ids) {
  if (length(task_ids) == 0) {
    return(invisible(logical(0)))
  }
  res <- con$pipeline(
    redux::redis$LRANGE(keys$queue, 0, -1),
    redux::redis$DEL(keys$queue))
  ids <- list_to_character(res[[1L]])
  keep <- !(ids %in% task_ids)
  if (any(keep)) {
    con$RPUSH(keys$queue, ids[keep])
  }
  invisible(task_ids %in% ids)
}
