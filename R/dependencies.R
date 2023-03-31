get_dependent_ids <- function(con, queue_id, task_id) {
  dependent_keys <- rrq_key_task_depends_down(queue_id, task_id)
  con$SMEMBERS(dependent_keys)
}

cancel_dependencies <- function(con, keys, store, ids) {
  dependent_keys <- rrq_key_task_depends_down(keys$queue_id, ids)
  dependent_ids <- unique(unlist(lapply(dependent_keys, con$SMEMBERS)))
  n <- length(dependent_ids)

  run_task_cleanup(con, keys, store, dependent_ids, TASK_IMPOSSIBLE, NULL)

  if (n > 0) {
    cancel_dependencies(con, keys, store, dependent_ids)
  }
}

queue_dependencies <- function(con, keys, task_id, deferred_task_ids) {
  dependency_keys <- rrq_key_task_depends_up(keys$queue_id, deferred_task_ids)
  res <- con$pipeline(.commands = c(
    lapply(dependency_keys, redis$SREM, task_id),
    set_names(lapply(dependency_keys, redis$SCARD), deferred_task_ids))
  )

  ## Tasks with 0 remaining dependencies can be queued
  tasks_to_queue <- names(res[res == 0 & names(res) != ""])
  if (length(tasks_to_queue > 0)) {
    task_queues <- list_to_character(con$HMGET(keys$task_queue, tasks_to_queue))
    queue_keys <- rrq_key_queue(keys$queue_id, task_queues)
    queue_task <- function(id, queue_key) {
      list(
        redis$SREM(keys$deferred_set, id),
        redis$LPUSH(queue_key, id),
        redis$HMSET(keys$task_status, id, TASK_PENDING)
      )
    }
    cmds <- Map(queue_task, tasks_to_queue, queue_keys)
    con$pipeline(.commands = unlist(cmds, FALSE, FALSE))
  }
}


deferred_list <- function(con, keys) {
  deferred_task_ids <- con$SMEMBERS(keys$deferred_set)
  deferred <- lapply(deferred_task_ids, function(deferred_task) {
    dependency_key <- rrq_key_task_depends_up_original(keys$queue_id,
                                                       deferred_task)
    deps <- con$SMEMBERS(dependency_key)
    set_names(con$HMGET(keys$task_status, deps), deps)
  })
  set_names(deferred, deferred_task_ids)
}
