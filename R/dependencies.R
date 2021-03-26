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
    queue_dependencies(worker$con, worker$keys, worker$queue_deferred,
                       task_id, dependent_ids)
  }
  invisible(TRUE)
}

cancel_dependencies <- function(con, keys, queue_deferred, ids) {
  n <- length(ids)
  con$pipeline(
    redis$HMSET(keys$task_status, ids, rep_len(TASK_IMPOSSIBLE, n)),
    redis$SREM(queue_deferred, ids)
  )
  dependent_keys <- rrq_key_task_dependents(keys$queue_id, ids)
  for (dependent_key in dependent_keys) {
    dependent_ids <- con$SMEMBERS(dependent_key)
    cancel_dependencies(con, keys, queue_deferred, dependent_ids)
  }
}

queue_dependencies <- function(con, keys, queue_deferred, task_id,
                               deferred_task_ids) {
  dependency_keys <- rrq_key_task_dependencies(keys$queue_id, deferred_task_ids)
  res <- con$pipeline(.commands = c(
    lapply(dependency_keys, redis$SREM, task_id),
    setNames(lapply(dependency_keys, redis$SCARD), deferred_task_ids))
  )

  ## Tasks with 0 remaining dependencies can be queued
  tasks_to_queue <- names(res[res == 0 & names(res) != ""])
  if (length(tasks_to_queue > 0)) {
    task_queues <- con$HMGET(keys$task_queue, tasks_to_queue)
    queue_keys <- rrq_key_queue(keys$queue_id, task_queues)
    queue_task <- function(id, queue_key) {
      list(
        redis$SREM(queue_deferred, id),
        redis$LPUSH(queue_key, id),
        redis$HMSET(keys$task_status, id, TASK_PENDING)
      )
    }
    cmds <- Map(queue_task, tasks_to_queue, queue_keys)
    con$pipeline(.commands = unlist(cmds, FALSE, FALSE))
  }
}
