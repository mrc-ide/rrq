worker_queue_dependencies <- function(con, keys, store, task_id, task_status) {
  dependent_keys <- rrq_key_task_dependents(keys$queue_id, task_id)
  dependent_ids <- con$SMEMBERS(dependent_keys)
  if (length(dependent_ids) == 0) {
    return()
  }

  if (identical(task_status, TASK_ERROR)) {
    cancel_dependencies(con, keys, store, task_id)
  } else {
    queue_dependencies(con, keys, task_id, dependent_ids)
  }
  invisible(TRUE)
}

cancel_dependencies <- function(con, keys, store, ids) {
  dependent_keys <- rrq_key_task_dependents(keys$queue_id, ids)
  dependent_ids <- unique(unlist(lapply(dependent_keys, con$SMEMBERS)))
  n <- length(dependent_ids)

  run_task_cleanup(
    con, keys, store, dependent_ids,
    rep(TASK_IMPOSSIBLE, length(dependent_ids)),
    rep(list(worker_task_failed(TASK_IMPOSSIBLE)), length(dependent_ids)))

  if (n > 0) {
    cancel_dependencies(con, keys, store, dependent_ids)
  }
}

queue_dependencies <- function(con, keys, task_id, deferred_task_ids) {
  dependency_keys <- rrq_key_task_dependencies(keys$queue_id, deferred_task_ids)
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
    dependency_key <- rrq_key_task_dependencies_original(keys$queue_id,
                                                         deferred_task)
    deps <- con$SMEMBERS(dependency_key)
    set_names(con$HMGET(keys$task_status, deps), deps)
  })
  set_names(deferred, deferred_task_ids)
}
