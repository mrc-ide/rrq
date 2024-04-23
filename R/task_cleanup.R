run_task_cleanup_success <- function(controller, task_id, status, value) {
  con <- controller$con
  keys <- controller$keys
  store <- controller$store

  task_result <- store$set(value, task_id)
  key_depends_down <- rrq_key_task_depends_down(keys$queue_id, task_id)
  res <- con$pipeline(
    redis$HSET(keys$task_result,        task_id, task_result),
    redis$HSET(keys$task_status,        task_id, status),
    redis$HSET(keys$task_time_complete, task_id, timestamp()),
    redis$RPUSH(rrq_key_task_complete(keys$queue_id, task_id), task_id),
    redis$SMEMBERS(key_depends_down))
  depends_down <- last(res)
  if (length(depends_down)) {
    queue_dependencies(controller, task_id, depends_down)
  }
}


## NOTE: unlike the success path (which is guaranteed to be a single
## task, single result) the failure path must be vectorised as we'll
## do bulk deletions quite frequently.
run_task_cleanup_failure <- function(controller, task_ids, status, value) {
  con <- controller$con
  keys <- controller$keys
  store <- controller$store

  ## TODO: we can do this more efficiently with some HMSET commands, I think
  cleanup_one <- function(task_id, status, value) {
    value <- value %||% worker_task_failed(status, keys$queue_id, task_id)
    task_result <- store$set(value, task_id)
    list(
      redis$HSET(keys$task_result,        task_id, task_result),
      redis$HSET(keys$task_status,        task_id, status),
      redis$HSET(keys$task_time_complete, task_id, timestamp()),
      redis$RPUSH(rrq_key_task_complete(keys$queue_id, task_id), task_id))
  }

  task_ids_all <- union(
    task_ids,
    unlist(task_depends_down(controller, task_ids), FALSE, FALSE))
  if (length(task_ids) < length(task_ids_all)) {
    n <- c(length(task_ids), length(task_ids_all) - length(task_ids))
    status <- rep(c(status, TASK_IMPOSSIBLE), n)
    value <- rep(list(value, NULL), n)
  } else {
    value <- rep(list(value), length(task_ids))
  }
  cmds <- Map(cleanup_one, task_ids_all, status, value)
  con$pipeline(.commands = unlist(cmds, FALSE, FALSE))
}
