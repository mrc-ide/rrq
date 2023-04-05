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
        redis$LPUSH(queue_key, id),
        redis$HMSET(keys$task_status, id, TASK_PENDING)
      )
    }
    cmds <- Map(queue_task, tasks_to_queue, queue_keys)
    con$pipeline(.commands = unlist(cmds, FALSE, FALSE))
  }
}


deferred_list <- function(con, keys) {
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
