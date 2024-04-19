##' Entirely destroy a queue, by deleting all keys associated with it
##' from the Redis database. This is a very destructive action and
##' cannot be undone.
##'
##' @title Destroy queue
##'
##' @param delete Either `TRUE` (the default) indicating that the
##'   keys should be immediately deleted. Alternatively, provide an
##'   integer value and the keys will instead be marked for future
##'   deletion by "expiring" after this many seconds, using Redis'
##'   `EXPIRE` command.
##'
##' @param worker_stop_type Passed to [rrq_worker_stop()]; Can be one
##'   of "message", "kill" or "kill_local". The "kill" method requires
##'   that the workers are using a heartbeat, and "kill_local"
##'   requires that the workers are on the same machine as the
##'   controller. However, these may be faster to stop workers than
##'   "message", which will wait until any task is finished.
##'
##' @param timeout_worker_stop A timeout to pass to the worker to
##'   respond the request to stop. See `worker_stop`'s `timeout`
##'   argument for details.
##'
##' @param controller The controller to destroy
##'
##' @export
rrq_destroy <- function(delete = TRUE, worker_stop_type = "message",
                        timeout_worker_stop = 0, controller = NULL) {
  controller <- get_controller(controller)
  con <- controller$con
  keys <- controller$keys
  queue_id <- keys$queue_id
  ## TODO: this can be made a bit less weird once we complete the
  ## refactor, as soon we'll have access to the controller everywhere.
  rrq_clean(con, queue_id, delete, worker_stop_type, timeout_worker_stop)
  if (identical(pkg$default_controller$queue_id, queue_id)) {
    rrq_default_controller_clear()
  }
  invisible()
}


##' Returns the length of the queue (the number of tasks waiting to
##' run).  This is the same as the length of the value returned by
##' [rrq_queue_list].
##'
##' @title Queue length
##'
##' @inheritParams rrq_queue_list
##'
##' @return A number
##'
##' @export
rrq_queue_length <- function(queue = NULL, controller = NULL) {
  controller <- get_controller(controller)
  con <- controller$con
  keys <- controller$keys
  key_queue <- rrq_key_queue(keys$queue_id, queue)
  con$LLEN(key_queue)
}


##' Returns the keys in the task queue.
##'
##' @title List queue contents
##'
##' @param queue The name of the queue to query (defaults to the
##'   "default" queue).
##'
##' @inheritParams rrq_task_list
##'
##' @export
rrq_queue_list <- function(queue = NULL, controller = NULL) {
  controller <- get_controller(controller)
  con <- controller$con
  keys <- controller$keys
  key_queue <- rrq_key_queue(keys$queue_id, queue)
  list_to_character(con$LRANGE(key_queue, 0, -1))
}


##' Remove task ids from a queue.
##'
##' @title Remove task ids from a queue
##'
##' @param task_ids Task ids to remove
##'
##' @param queue The name of the queue to query (defaults to the
##'   "default" queue).
##'
##' @inheritParams rrq_task_times
##'
##' @export
rrq_queue_remove <- function(task_ids, queue = NULL, controller = NULL) {
  ## NOTE: uses a pipeline to avoid a race condition - nothing may
  ## interere with the queue between the LRANGE and the DEL or we
  ## might lose tasks or double-queue them. If a job is queued
  ## between the DEL and the RPUSH the newly submitted job gets
  ## bounced ahead in the queue, which seems tolerable but might not
  ## always be ideal.  To solve this we should use a lua script.
  controller <- get_controller(controller)
  con <- controller$con
  keys <- controller$keys

  if (length(task_ids) == 0) {
    return(invisible(logical(0)))
  }
  if (is.null(queue)) {
    queue <- QUEUE_DEFAULT
  }
  if (length(queue) > 1) {
    ## TODO: Document these semantics
    tmp <- split(task_ids, queue)
    res <- Map(function(i, q) i[rrq_queue_remove(i, q, controller)],
               tmp, names(tmp))
    return(invisible(task_ids %in% unlist(res)))
  }
  key_queue <- rrq_key_queue(keys$queue_id, queue)
  res <- con$pipeline(
    redux::redis$LRANGE(key_queue, 0, -1),
    redux::redis$DEL(key_queue))
  ids <- list_to_character(res[[1L]])
  keep <- !(ids %in% task_ids)
  if (any(keep)) {
    con$RPUSH(key_queue, ids[keep])
  }
  invisible(task_ids %in% ids)
}


##' Return deferred tasks and what they are waiting on.
##'   Note this is in an arbitrary order, tasks will be added to the
##'   queue as their dependencies are satisfied.
##'
##' @title List deferred tasks
##'
##' @inheritParams rrq_task_list
##'
##' @export
rrq_deferred_list <- function(controller = NULL) {
  controller <- get_controller(controller)
  con <- controller$con
  keys <- controller$keys

  status <- redux::scan_find(con, "*", type = "HSCAN", key = keys$task_status)
  deferred_task_ids <- status[, "field"][status[, "value"] == TASK_DEFERRED]
  tmp <- rrq_key_task_depends_up_original(keys$queue_id, deferred_task_ids)
  deps <- lapply(
    con$pipeline(.commands = lapply(tmp, redis$SMEMBERS)),
    list_to_character)
  deps_unique <- unique(unlist(deps))
  deps_unique_status <- from_redis_hash(con, keys$task_status, deps_unique)
  set_names(lapply(deps, function(d) deps_unique_status[d]),
            deferred_task_ids)
}
