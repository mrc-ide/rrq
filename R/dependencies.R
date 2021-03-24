worker_queue_dependencies <- function(worker, task_id, task_status) {
  dependent_keys <- rrq_key_task_dependents(worker$keys$queue_id, task_id)
  dependent_ids <- worker$con$SMEMBERS(dependent_keys)
  if (length(dependent_ids) == 0) {
    return()
  }

  if (identical(task_status, TASK_ERROR)) {
    cancel_dependencies(worker$con, worker$keys, worker$queue_deferred,
                        dependent_ids)
  } else {
    queue_dependencies(worker$con, worker$keys, worker$queue,
                       worker$queue_deferred, task_id, dependent_ids)
  }
  invisible(TRUE)
}

cancel_dependencies <- function(con, keys, queue_deferred, ids) {
  n <- length(ids)
  con$pipeline(
    redis$HMSET(keys$task_status, ids, rep_len(TASK_IMPOSSIBLE, n)),
    redis$SREM(queue_deferred, ids)
  )
}

queue_dependencies <- function(con, keys, queue, queue_deferred,
                               task_id, deferred_task_ids) {
  move_to_queue <- '
    local key_dependencies_set = KEYS[1]
    local key_queue = KEYS[2]
    local key_deferred_queue = KEYS[3]
    local key_task_status = KEYS[4]
    local task_id = ARGV[1]
    local deferred_task_id = ARGV[2]
    local task_pending_status = ARGV[3]
    redis.call("SREM", key_dependencies_set, task_id)
    local cardinality = redis.call("SCARD", key_dependencies_set)
    if (cardinality == 0) then
      redis.call("SREM", key_deferred_queue, deferred_task_id)
      redis.call("LPUSH", key_queue, deferred_task_id)
      redis.call("HMSET", key_task_status, deferred_task_id,
                 task_pending_status)
    end
  '
  scripts <- redux::redis_scripts(con, move_to_queue = move_to_queue)
  for (id in deferred_task_ids) {
    dependency_key <- rrq_key_task_dependencies(keys$queue_id, id)
    scripts("move_to_queue",
            c(dependency_key, queue, queue_deferred, keys$task_status),
            c(task_id, id, TASK_PENDING))
  }
}
