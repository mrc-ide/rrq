run_task_cleanup <- function(con, keys, store, task_ids, status, value) {
  cleanup_one <- function(task_id) {
    value <- value %||% worker_task_failed(status, keys$queue_id, task_id)
    task_result <- store$set(value, task_id)
    key_complete <- con$HGET(keys$task_complete, task_id)
    list(
      redis$HSET(keys$task_result,        task_id, task_result),
      redis$HSET(keys$task_status,        task_id, status),
      redis$HSET(keys$task_time_complete, task_id, timestamp()),
      redis$RPUSH(rrq_key_task_complete(keys$queue_id, task_id), task_id),
      if (!is.null(key_complete)) {
        redis$RPUSH(key_complete, task_id)
      },
      redis$SREM(keys$deferred_set, task_id)
    )
  }
  cmds <- Map(cleanup_one, task_ids)
  con$pipeline(.commands = unlist(cmds, FALSE, FALSE))
}
