##' List all tasks.  This may be a lot of tasks, and so can be quite
##' slow to execute.
##'
##' @title List all tasks
##'
##' @param controller The controller to use.  If not given (or `NULL`)
##'   we'll use the controller registered with
##'   [rrq_default_controller_set()].
##'
##' @return A character vector
##'
##' @export
##' @examplesIf rrq:::enable_examples()
##'
##' obj <- rrq_controller("rrq:example")
##'
##' rrq_task_list(controller = obj)
rrq_task_list <- function(controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  as.character(con$HKEYS(keys$task_expr))
}


##' Provide a high level overview of task statuses for a set of task
##' ids, being the count in major categories of `PENDING`, `RUNNING`,
##' `COMPLETE`, `ERROR`, `CANCELLED`, `DIED`, `TIMEOUT`, `IMPOSSIBLE`,
##' `DEFERRED` and `MOVED`.
##'
##' @title High level task overview
##'
##' @param task_ids Optional character vector of task ids for which
##'   you would like the overview. If not given (or `NULL`) then the
##'   status of all task ids known to this rrq controller is used
##'   (this might be fairly costly).
##'
##' @inheritParams rrq_task_list
##'
##' @return A list with names corresponding to possible task status
##'   levels and values being the number of tasks in that state.
##'
##' @export
##' @examplesIf rrq:::enable_examples()
##'
##' obj <- rrq_controller("rrq:example")
##'
##' ids <- rrq_task_list(controller = obj)
##' t(as.data.frame(rrq_task_overview(ids, controller = obj)))
rrq_task_overview <- function(task_ids = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  if (is.null(task_ids)) {
    status <- from_redis_hash(controller$con, controller$keys$task_status)
  } else {
    status <- rrq_task_status(task_ids, controller = controller)
  }
  lvls <- c(TASK$all, setdiff(unique(status), TASK$all))
  as.list(table(factor(status, lvls)))
}


##' Test if task ids exist (i.e., are known to this controller).
##' Nonexistent tasks may be deleted, known to a different controller
##' or just never have existed.
##'
##' @title Test if tasks exist
##'
##' @param task_ids Vector of task ids to check
##'
##' @param named Logical, indicating if the return value should be
##'   named with the task ids; as these are quite long this can make
##'   the value a little awkward to work with.
##'
##' @inheritParams rrq_task_list
##'
##' @return A logical vector the same length as task_ids; `TRUE` where
##'   the task exists, `FALSE` otherwise.  If `named` was `TRUE`, then
##'   this vector is named with `task_ids`.
##'
##' @export
##' @examplesIf rrq:::enable_examples()
##' obj <- rrq_controller("rrq:example")
##'
##' t1 <- rrq_task_create_expr(runif(1), controller = obj)
##' rrq_task_exists(t1, controller = obj)
##'
##' t2 <- ids::random_id()
##' rrq_task_exists(t2, controller = obj)
rrq_task_exists <- function(task_ids, named = FALSE, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  assert_character(task_ids, call = rlang::current_env())
  if (length(task_ids) == 0) {
    exists <- logical(0)
  } else {
    expr <- controller$con$HMGET(controller$keys$task_expr, task_ids)
    exists <- !vlapply(expr, is.null)
  }
  if (named) set_names(exists, task_ids) else exists
}


##' Fetch information about a task. This currently includes
##' information about where a task is (or was) running and information
##' about any retry chain, but will expand in future. The format of
##' the output here is subject to change (and will probably get a nice
##' print method) but the values present in the output will be
##' included in any future update.
##'
##' @title Fetch task information
##'
##' @param task_id A single task identifier
##'
##' @inheritParams rrq_task_list
##'
##' @return A list, format currently subject to change
##'
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' # Get information about a task
##' t <- rrq_task_create_expr(runif(1), controller = obj)
##' rrq_task_info(t, controller = obj)
##'
##' # If the task has been retried, the retry chain is shown
##' rrq_task_wait(t, controller = obj)
##' rrq_task_retry(t, controller = obj)
##' rrq_task_info(t, controller = obj)
##'
##' # If the task was queued onto a separate process, then this
##' # information is shown
##' rrq_task_create_expr(1 + 1, separate_process = TRUE, timeout_task_run = 60,
##'                       controller = obj)
##' rrq_task_wait(t, controller = obj)
##' rrq_task_info(t, controller = obj)
rrq_task_info <- function(task_id, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_scalar_character(task_id, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  assert_scalar_character(task_id, call = rlang::current_env())
  dat <- con$pipeline(
    status = redis$HGET(keys$task_status, task_id),
    queue = redis$HGET(keys$task_queue, task_id),
    local = redis$HGET(keys$task_local, task_id),
    timeout = redis$HGET(keys$task_timeout, task_id),
    worker = redis$HGET(keys$task_worker, task_id),
    root = redis$HGET(keys$task_moved_root, task_id),
    pid = redis$HGET(keys$task_pid, task_id))

  moved <- list(up = NULL, down = NULL)

  if (dat$status == TASK_MOVED && is.null(dat$root)) {
    dat$root <- task_id
  }
  if (!is.null(dat$root)) {
    chain <- task_follow_chain(controller, dat$root)[[1L]]
    pos <- which(chain == task_id)
    if (pos > 1) {
      moved$up <- chain[seq_len(pos - 1)]
    }
    if (pos < length(chain)) {
      moved$down <- chain[seq.int(pos + 1, length(chain))]
    }
  }

  depends <- list(up = task_depends_up(controller, task_id),
                  down = task_depends_down(controller, task_id))

  list(
    id = task_id,
    status = dat$status,
    queue = dat$queue,
    separate_process = dat$local == "FALSE",
    timeout = dat$timeout %&&% as.numeric(dat$timeout),
    worker = dat$worker,
    pid = dat$pid %&&% as.integer(dat$pid),
    depends = depends,
    moved = moved)
}


##' Fetch internal data about a task (expert use only)
##'
##' @title Fetch internal task data
##'
##' @inheritParams rrq_task_info
##'
##' @return Internal data, structures subject to change
##'
##' @export
##' @examplesIf rrq:::enable_examples()
##'
##' obj <- rrq_controller("rrq:example")
##'
##' t <- rrq_task_create_expr(runif(1), controller = obj)
##' rrq_task_data(t, controller = obj)
##'
##' x <- 10
##' y <- 20
##' t <- rrq_task_create_expr(x + y, controller = obj)
##' rrq_task_data(t, controller = obj)
rrq_task_data <- function(task_id, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_scalar_character(task_id, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  store <- controller$store

  expr <- con$HGET(keys$task_expr, task_id)
  if (is.null(expr)) {
    stop(sprintf("Task '%s' not found", task_id))
  } else if (is_task_redirect(expr)) {
    return(rrq_task_data(expr, controller))
  }
  task <- bin_to_object(expr)
  task <- task_load_from_store(task, store)
  task
}


##' Fetch times for tasks at points in their life cycle.  For each
##' task returns the time of submission, starting and completion (not
##' necessarily successfully; this includes errors and interruptions).
##' If a task has not reached a point yet (e.g., submitted but not
##' run, or running but not finished) the time will be `NA`).  Times
##' are returned in unix timestamp format in UTC; you can use
##' [redux::redis_time_to_r] to convert them to a POSIXt object.
##'
##' @title Fetch task times
##'
##' @param task_ids A vector of task ids
##'
##' @param follow Optional logical, indicating if we should follow any
##'   redirects set up by doing [rrq_task_retry]. If not given, falls
##'   back on the value passed into the controller, the global option
##'   `rrq.follow`, and finally `TRUE`. Set to `FALSE` if you want to
##'   return information about the original task, even if it has been
##'   subsequently retried.
##'
##' @inheritParams rrq_task_list
##'
##' @return A matrix of times, with row names corresponding to task
##'   ids.  We may change this to a data.frame at some point in the
##'   future.
##'
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' t <- rrq_task_create_expr(Sys.sleep(3), controller = obj)
##' rrq_task_times(t, controller = obj)
##' rrq_task_wait(t, controller = obj)
##' rrq_task_times(t, controller = obj)
rrq_task_times <- function(task_ids, follow = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_character(task_ids, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  if (follow %||% controller$follow) {
    task_ids_from <- task_follow(controller, task_ids)
  } else {
    task_ids_from <- task_ids
  }
  read_time_with_default <- function(key) {
    if (length(task_ids) == 0) {
      return(numeric(0))
    }
    time <- con$HMGET(key, task_ids_from)
    time[vlapply(time, is.null)] <- NA_character_
    as.numeric(list_to_character(time))
  }
  ret <- cbind(
    submit = read_time_with_default(keys$task_time_submit),
    start = read_time_with_default(keys$task_time_start),
    complete = read_time_with_default(keys$task_time_complete),
    moved = read_time_with_default(keys$task_time_moved))
  rownames(ret) <- task_ids
  ret
}


##' Get the result for a single task (see [rrq_task_results] for a
##' method for efficiently getting multiple results at once).  Returns
##' the value of running the task if it is complete, and an error
##' otherwise.
##'
##' @title Fetch single task result
##'
##' @param task_id The single id for which the result is wanted.
##'
##' @param error Logical, indicating if we should throw an error if a
##'   task was not successful. By default (`error = FALSE`), in the
##'   case of the task result returning an error we return an object
##'   of class `rrq_task_error`, which contains information about the
##'   error. Passing `error = TRUE` calls `stop()` on this error if it
##'   is returned.
##'
##' @inheritParams rrq_task_times
##'
##' @return The result of your task.  This may be an error (an object
##'   with class `rrq_task_error`) if your task has failed.
##'
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' # Create a task, wait for it to finish and fetch its result
##' t <- rrq_task_create_expr(runif(1), controller = obj)
##' rrq_task_wait(t, controller = obj)
##' rrq_task_result(t, controller = obj)
##'
##' # Tasks that fail do not fail on result, but instead return an
##' # object with the class "rrq_task_error"
##' t <- rrq_task_create_expr(readRDS("somefile.rds"), controller = obj)
##' rrq_task_wait(t, controller = obj)
##' rrq_task_result(t, controller = obj)
rrq_task_result <- function(task_id, error = FALSE, follow = NULL,
                            controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_scalar_character(task_id, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  store <- controller$store
  follow <- follow %||% controller$follow

  task_id_from <- if (follow) task_follow(controller, task_id) else task_id

  hash <- con$HGET(keys$task_result, task_id_from)
  if (is.null(hash)) {
    stop(sprintf("Missing result for task: '%s'", task_id),
         call. = FALSE)
  }
  res <- store$get(hash)
  if (error && inherits(res, "rrq_task_error")) {
    stop(res)
  }
  res
}


##' Get the results of a group of tasks, returning them as a list.
##' See [rrq_task_result] for getting the result of a single task.
##'
##' @param task_ids A vector of task ids for which the task result
##' is wanted.
##'
##' @param error Logical, indicating if we should throw an error if
##'   the task was not successful. See [rrq_task_result()] for details.
##'
##' @inheritParams rrq_task_times
##' @inheritParams rrq_task_exists
##'
##' @return A list, one entry per result.  This function errors if
##'   any task is not available.  If `named = TRUE`, then this list is
##'   named with the `task_ids`.
##'
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' ts <- rrq_task_create_bulk_call(sqrt, 1:10, controller = obj)
##' rrq_task_wait(ts, controller = obj)
##' rrq_task_results(ts, controller = obj)
##'
##' # For a single task, rrq_task_result and rrq_task_results differ
##' # in the return type; rrq_task_results always returns a list:
##' t <- ts[[1]]
##' rrq_task_result(t, controller = obj)
##' rrq_task_results(t, controller = obj)
rrq_task_results <- function(task_ids, error = FALSE, named = FALSE,
                             follow = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_character(task_ids, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  store <- controller$store
  follow <- follow %||% controller$follow

  task_ids_from <- if (follow) task_follow(controller, task_ids) else task_ids
  hash <- from_redis_hash(con, keys$task_result, task_ids_from)
  is_missing <- is.na(hash)
  if (any(is_missing)) {
    stop(sprintf("Missing result for task:\n%s",
                 paste(sprintf("  - %s", task_ids[is_missing]),
                       collapse = "\n")),
         call. = FALSE)
  }
  res <- store$mget(hash)
  if (error) {
    is_error <- vlapply(res, inherits, "rrq_task_error")
    if (any(is_error)) {
      stop(rrq_task_error_group(unname(res[is_error]), length(res)))
    }
  }
  if (named) set_names(res, task_ids) else res
}


##' Return a character vector of task statuses. The name of each
##' element corresponds to a task id, and the value will be one of the
##' possible statuses ("PENDING", "COMPLETE", etc).
##'
##' @title Fetch task statuses
##'
##' @param task_ids Optional character vector of task ids for which you
##' would like statuses.
##'
##' @inheritParams rrq_task_times
##' @inheritParams rrq_task_exists
##'
##' @return A character vector the same length as `task_ids`
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' ts <- rrq_task_create_bulk_call(sqrt, 1:10, controller = obj)
##' rrq_task_status(ts, controller = obj)
##' rrq_task_wait(ts, controller = obj)
##' rrq_task_status(ts, controller = obj)
rrq_task_status <- function(task_ids, named = FALSE, follow = NULL,
                            controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_character(task_ids, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  follow <- follow %||% controller$follow

  if (length(task_ids) == 0) {
    status <- character()
  } else {
    status <- hash_result_to_character(
      con$HMGET(keys$task_status, task_ids),
      missing = TASK_MISSING)
  }
  if (follow && any(is_moved <- status == TASK_MOVED)) {
    task_ids_moved <- task_follow(controller, task_ids[is_moved])
    status[is_moved] <- rrq_task_status(task_ids_moved, follow = FALSE,
                                        controller = controller)
  }
  if (named) set_names(status, task_ids) else status
}


##' Retrieve task progress, if set. This will be `NULL` if progress
##' has never been registered, otherwise whatever value was set - can
##' be an arbitrary R object.
##'
##' @title Fetch task progress information
##'
##' @param task_id A single task id for which the progress is wanted.
##'
##' @inheritParams rrq_task_list
##'
##' @return Any set progress object
##' @export
rrq_task_progress <- function(task_id, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_scalar_character(task_id, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  ret <- con$HGET(keys$task_progress, task_id)
  if (!is.null(ret)) {
    ret <- bin_to_object(ret)
  }
  ret
}


##' Find the position of one or more tasks in the queue.
##'
##' @title Find task position in queue
##'
##' @param task_ids Character vector of tasks to find the position for.
##'
##' @param missing Value to return if the task is not found in the queue.
##'   A task will take value `missing` if it is running, complete,
##'   errored, deferred etc and a positive integer if it is in the queue,
##'   indicating its position (with 1) being the next task to run.
##'
##' @param queue The name of the queue to query (defaults to the
##'   "default" queue).
##'
##' @inheritParams rrq_task_times
##'
##' @return An integer vector, the same length as `task_ids`
##'
##' @export
rrq_task_position <- function(task_ids, missing = 0L, queue = NULL,
                              follow = NULL, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  ## TODO: validate missing - integer or NA?
  assert_character(task_ids, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  follow <- follow %||% controller$follow

  ## NOTE: This is not crazy efficient; we pull the entire list down
  ## which is not ideal.  However, in practice it seems fairly fast.
  ## But one should be careful to adjust the polling interval of
  ## something usnig this not to flood the server with excessive load.
  ##
  ## A better way would possibly be to use a LUA script; especially for
  ## the case where there is a single job that'd be fairly easy to do.
  key_queue <- rrq_key_queue(keys$queue_id, queue)
  queue_contents <- vcapply(con$LRANGE(key_queue, 0, -1L), identity)
  if (follow && length(queue_contents) > 0L) {
    ## In some ways following is the only thing that makes sense here,
    ## as only the last id in the chain can possibly be queued.
    task_ids <- task_follow(controller, task_ids)
  }
  match(task_ids, queue_contents, missing)
}


##' List the tasks in front of `task_id` in the queue.
##'   If the task is missing from the queue this will return NULL. If
##'   the task is next in the queue this will return an empty character
##'   vector.
##'
##' @title List tasks ahead of a task
##'
##' @param task_id Task to find the position for.
##'
##' @param queue The name of the queue to query (defaults to the
##'   "default" queue).
##'
##' @inheritParams rrq_task_times
##' @export
rrq_task_preceeding <- function(task_id, queue = NULL, follow = NULL,
                                controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_scalar_character(task_id, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  follow <- follow %||% controller$follow

  key_queue <- rrq_key_queue(keys$queue_id, queue)
  queue_contents <- vcapply(con$LRANGE(key_queue, 0, -1L), identity)
  if (follow && length(queue_contents) > 0L) {
    ## In some ways following is the only thing that makes sense here,
    ## as only the last id in the chain can possibly be queued.
    task_id <- task_follow(controller, task_id)
  }
  task_position <- match(task_id, queue_contents)
  if (is.na(task_position)) {
    return(NULL)
  }
  queue_contents[seq_len(task_position - 1)]
}


##' Delete one or more tasks
##'
##' @title Delete tasks
##'
##' @param task_ids Vector of task ids to delete
##'
##' @param check Logical indicating if we should check that the tasks
##'   are not running. Deleting running tasks is unlikely to result in
##'   desirable behaviour.
##'
##' @inheritParams rrq_task_list
##' @export
##' @return Nothing, called for side effects only
##' @examplesIf rrq:::enable_examples()
##' obj <- rrq_controller("rrq:example:delete")
##'
##' ts <- rrq_task_create_bulk_call(sqrt, 1:10, controller = obj)
##' rrq_task_exists(ts, controller = obj)
##'
##' rrq_task_delete(ts[1:5], controller = obj)
##' rrq_task_exists(ts, controller = obj)
##'
##' rrq_task_delete(ts, controller = obj)
##' rrq_task_exists(ts, controller = obj)
rrq_task_delete <- function(task_ids, check = TRUE, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_character(task_ids, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys
  store <- controller$store

  task_chain <- task_follow_chain(controller, task_ids)
  task_ids_root <- vcapply(task_chain, first, USE.NAMES = FALSE)
  task_ids_all <- unlist(task_chain)

  if (check) {
    st <- from_redis_hash(con, keys$task_status, task_ids_all,
                          missing = TASK_MISSING)
    if (any(st == TASK_RUNNING)) {
      ## This already is not a great error, but will be even harder to
      ## understand if the user is deleting a task that has been
      ## retried.
      stop("Can't delete running tasks")
    }
  }

  depends_up_original_keys <- rrq_key_task_depends_up_original(
    keys$queue_id, task_ids_all)
  depends_up_keys <- rrq_key_task_depends_up(keys$queue_id, task_ids_all)
  depends_down_keys <- rrq_key_task_depends_down(keys$queue_id, task_ids_all)
  res <- con$pipeline(
    .commands = c(
      lapply(depends_down_keys, redis$SCARD),
      list(
        redis$HMGET(keys$task_status,  task_ids_all),
        redis$HDEL(keys$task_expr,     task_ids_all),
        redis$HDEL(keys$task_status,   task_ids_all),
        redis$HDEL(keys$task_result,   task_ids_all),
        redis$HDEL(keys$task_complete, task_ids_all),
        redis$HDEL(keys$task_progress, task_ids_all),
        redis$HDEL(keys$task_worker,   task_ids_all),
        redis$HDEL(keys$task_local,    task_ids_all),
        redis$DEL(depends_up_original_keys),
        redis$DEL(depends_up_keys))))

  queue <- list_to_character(con$HMGET(keys$task_queue, task_ids_root))
  rrq_queue_remove(task_ids_all, queue, controller)

  store$drop(task_ids_all)

  ## We only want to cancel dependencies i.e. set status to IMPOSSIBLE when
  ## A. They are dependents of a task which is PENDING or DEFERRED AND
  ## B. Their dependencies have not already been deleted or set to ERRORED, etc.
  ## i.e. their dependencies are also DEFERRED
  n <- length(task_ids_all)
  check_dependencies <-
    (list_to_numeric(res[seq_len(n)]) > 0) &
    vlapply(res[[n + 1]], function(x) !is.null(x) && x %in% TASK$unstarted)
  if (any(check_dependencies)) {
    ids_all_deps <- unlist(
      task_depends_down(controller, task_ids_all[check_dependencies]),
      FALSE, FALSE)
    ids_deps <- setdiff(ids_all_deps, task_ids_all)
    status_deps <- rrq_task_status(ids_deps, follow = FALSE,
                                   controller = controller)
    ids_impossible <- ids_deps[status_deps == TASK_DEFERRED]
    if (length(ids_impossible) > 0) {
      run_task_cleanup_failure(controller, ids_impossible, TASK_IMPOSSIBLE,
                               NULL)
    }
  }

  con$DEL(depends_down_keys)

  invisible()
}


##' Cancel a single task. If the task is `PENDING` it will be unqueued
##' and the status set to `CANCELED`.  If `RUNNING` then the task will
##' be stopped if it was set to run in a separate process (i.e.,
##' queued with `separate_process = TRUE`).  Dependent tasks will be
##' marked as impossible.
##'
##' @title Cancel a task
##'
##' @param task_id Id of the task to cancel
##'
##' @param wait Wait for the task to be stopped, if it was running.
##'
##' @param timeout_wait Maximum time, in seconds, to wait for the task
##'   to be cancelled by the worker.
##'
##' @inheritParams rrq_task_list
##'
##' @return Nothing if successfully cancelled, otherwise throws an
##' error with task_id and status e.g. Task 123 is not running (MISSING)
##'
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' t <- rrq_task_create_expr(Sys.sleep(4), separate_process = TRUE,
##'                           controller = obj)
##' Sys.sleep(0.5)
##' rrq_task_cancel(t, controller = obj)
##' rrq_task_status(t, controller = obj)
rrq_task_cancel <- function(task_id, wait = TRUE, timeout_wait = 10,
                            controller = NULL) {
  ## TODO: several legacy issues here:
  ## * why not use our general timeout?
  ## * why not cancel several at once?
  controller <- get_controller(controller, call = rlang::current_env())
  assert_scalar_character(task_id, call = rlang::current_env())
  assert_scalar_logical(wait)
  assert_valid_timeout(timeout_wait)
  con <- controller$con
  keys <- controller$keys
  store <- controller$store

  ## There are several steps here, which will all be executed in one
  ## block which removes the possibility of race conditions:
  ##
  ## * Remove the task_id from its queue (whichever it is in) so that
  ##   it cannot be picked up by any worker (prevents status moving
  ##   from PENDING -> RUNNING)
  ##
  ## * Mark the job as cancelled so that if it is running on a
  ##   separate process it will be eligible to be stopped as soon as
  ##   possible.
  ##
  ## * Determine if it is a local or a separate process task so we
  ## * know if it will be cancelled if it was running.
  ##
  ## * Retrieve the status so that we know the task status before any
  ##   change can happen.
  ##
  ## Unfortunately it's not possible to also cancel dependencies in a
  ## race-free way and we'll tidy that up later.
  key_queue <- rrq_key_queue(keys$queue_id, con$HGET(keys$task_queue, task_id))

  dat <- con$pipeline(
    dropped = redis$LREM(key_queue, 1, task_id),
    cancel = redis$HSET(keys$task_cancel, task_id, "TRUE"),
    status = redis$HGET(keys$task_status, task_id),
    local = redis$HGET(keys$task_local, task_id))

  task_status <- dat$status %||% TASK_MISSING
  if (!(task_status %in% TASK$unfinished)) {
    stop(sprintf("Task %s is not cancelable (%s)", task_id, task_status))
  }

  if (task_status == TASK_RUNNING) {
    if (dat$local != "FALSE") {
      stop(sprintf(
        "Can't cancel running task '%s' as not in separate process", task_id))
    }
    if (wait) {
      wait_status_change(controller, task_id, TASK_RUNNING, timeout_wait)
    }
  } else {
    run_task_cleanup_failure(controller, task_id, TASK_CANCELLED, NULL)
  }

  invisible(NULL)
}


##' Wait for a task, or set of tasks, to complete.  If you have used
##' `rrq` prior to version 0.8.0, you might expect this function to
##' return the result, but we now return a logical value which
##' indicates success or not.  You can fetch the task result with
##' [rrq_task_result].
##'
##' @title Wait for group of tasks
##'
##' @param task_id A vector of task ids to poll for (can be one task
##'   or many)
##'
##' @param timeout Optional timeout, in seconds, after which an error
##'   will be thrown if the task has not completed. If not given,
##'   falls back on the controller's `timeout_task_wait` (see
##'   [rrq_controller])
##'
##' @param time_poll Optional time with which to "poll" for
##'   completion.  By default this will be 1 second; this is the time
##'   that each request for a completed task may block for (however,
##'   if the task is finished before this, the actual time waited for
##'   will be less).  Increasing this will reduce the responsiveness
##'   of your R session to interrupting, but will cause slightly less
##'   network load.  Values less than 1s are only supported with Redis
##'   server version 6.0.0 or greater (released September 2020).
##'
##' @param progress Optional logical indicating if a progress bar
##'   should be displayed. If `NULL` we fall back on the value of the
##'   global option `rrq.progress`, and if that is unset display a
##'   progress bar if in an interactive session.
##'
##' @inheritParams rrq_task_times
##'
##' @return A scalar logical value; `TRUE` if _all_ tasks complete
##'   successfully and `FALSE` otherwise
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##' t1 <- rrq_task_create_expr(Sys.sleep(1), controller = obj)
##' rrq_task_wait(t1, controller = obj)
##'
##' # The return value of wait gives a summary of successfullness
##' # of the task
##' t2 <- rrq_task_create_expr(stop("Some error"), controller = obj)
##' rrq_task_wait(t2, controller = obj)
##'
##' # If you wait on many tasks, the return value is effectively
##' # reduced with "all" (so the result is TRUE if all tasks were
##' # successful)
##' rrq_task_wait(c(t1, t2), controller = obj)
rrq_task_wait <- function(task_id, timeout = NULL, time_poll = 1,
                          progress = NULL, follow = NULL,
                          controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_character(task_id)
  con <- controller$con
  keys <- controller$keys
  store <- controller$store
  timeout <- timeout %||% controller$timeout_task_wait
  follow <- follow %||% controller$follow
  time_poll <- validate_time_poll(con, time_poll, call = rlang::current_env())

  if (length(task_id) == 0) {
    cli::cli_abort("Can't wait on no tasks")
  }

  task_id_from <- if (follow) task_follow(controller, task_id) else task_id
  status <- hash_result_to_character(con$HMGET(keys$task_status, task_id_from),
                                     TASK_MISSING)
  if (any(status == TASK_MISSING)) {
    cli::cli_abort("Can't wait on missing tasks")
  }

  key_complete <- rrq_key_task_complete(keys$queue_id, task_id_from)
  incomplete <- c(TASK_PENDING, TASK_DEFERRED, TASK_RUNNING)

  get_status <- function() {
    waiting <- status %in% incomplete
    if (any(waiting)) {
      res <- con$pipeline(
        redis$BLPOP(key_complete[waiting], time_poll),
        redis$HMGET(keys$task_status, task_id_from[waiting]))
      status[waiting] <<- hash_result_to_character(res[[2]], TASK_MISSING)
    }
    status
  }

  if (any(status %in% incomplete)) {
    multiple <- length(task_id) > 1
    name <- if (multiple) "tasks" else "task"
    res <- logwatch::logwatch(
      name,
      get_status = get_status,
      get_log = NULL,
      show_log = FALSE,
      multiple = multiple,
      show_spinner = show_progress(progress),
      poll = 0,
      timeout = timeout,
      status_waiting = c(TASK_PENDING, TASK_DEFERRED),
      status_running = TASK_RUNNING,
      status_timeout = "wait:timeout",
      status_interrupt = "wait:interrupt")
    if (any(res$status %in% c("wait:timeout", "wait:interrupt"))) {
      cli::cli_abort("{name} did not complete in time")
    }
    status <- res$status
  }
  con$DEL(key_complete)
  all(status == TASK_COMPLETE)
}


##' Fetch logs from tasks that were queued into separate processes
##' (e.g., with [rrq_task_create_expr] using `separate_process =
##' TRUE`).  It is not knowable if a task definitely produces logs - if
##' you have a mixture of workers that do enable task logs and some
##' that don't, then it will depend on the worker that picks it up if
##' logging will be enabled.  Don't do this though and you should be
##' fine.
##'
##' @title Fetch task logs
##'
##' @inheritParams rrq_task_info
##'
##' @return A character vector of logs, or `NULL` if no log is present
##'   yet.  If logging is not enabled for this task, we throw an
##'   error.  Empty logs can be distinguished from "no logs yet", as
##'   they will return an empty character vector (`character(0)`).
##'
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##'
##' obj <- rrq_controller("rrq:example")
##'
##' t <- rrq_task_create_expr(message("hello!"), separate_process = TRUE,
##'                           controller = obj)
##' rrq_task_wait(t, controller = obj)
##' rrq_task_log(t, controller = obj)
rrq_task_log <- function(task_id, controller = NULL) {
  controller <- get_controller(controller, call = rlang::current_env())
  assert_scalar_character(task_id, call = rlang::current_env())
  con <- controller$con
  keys <- controller$keys

  dat <- con$pipeline(
    filename = redis$HGET(keys$task_logfile, task_id),
    local = redis$HGET(keys$task_local, task_id))
  filename <- dat$filename

  logs <- NULL
  if (is.null(filename)) {
    if (is.null(dat$local)) {
      cli::cli_abort("Task '{task_id}' does not exist")
    }
    separate_process <- dat$local == "FALSE"
    if (!separate_process) {
      cli::cli_abort(
        c(paste("Task log not enabled for '{task_id}', as it was not",
                "configured to run in a separate process"),
          i = paste("Queue your tasks with 'separate_process = TRUE' to",
                    "enable task logs")))
    }
  } else if (file.exists(filename)) {
    logs <- readLines(filename)
  }

  logs
}


task_load_from_store <- function(task, store) {
  if (task$type == "expr") {
    if (!is.null(task$variables)) {
      if (is.null(task$variable_in_store)) {
        task$variables <- set_names(store$mget(task$variables),
                                    names(task$variables))
      } else if (any(task$variable_in_store)) {
        task$variables[task$variable_in_store] <-
          store$mget(task$variables[task$variable_in_store])
      }
    }
  } else {
    task$fn <- store$get(task$fn)
    if (is.null(task$arg_in_store)) {
      task$args <- set_names(store$mget(task$args), names(task$args))
    } else if (any(task$arg_in_store)) {
      task$args[task$arg_in_store] <- store$mget(task$args[task$arg_in_store])
    }
  }
  task
}
