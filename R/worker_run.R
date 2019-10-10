worker_run_task <- function(worker, task_id) {
  dat <- worker_run_task_start(worker, task_id)
  task <- dat$task
  e <- expression_restore_locals(task, worker$envir, worker$db)
  res <- expression_eval_safely(task$expr, e)
  task_status <- if (res$success) TASK_COMPLETE else TASK_ERROR
  worker_run_task_cleanup(worker, task_id, task_status, res$value,
                          dat$key_complete)
}


worker_run_task_start <- function(worker, task_id) {
  keys <- worker$keys
  name <- worker$name
  dat <- worker$con$pipeline(
    worker_log(redis, keys, "TASK_START", "task_id"),
    redis$HSET(keys$worker_status, name,      WORKER_BUSY),
    redis$HSET(keys$worker_task,   name,      task_id),
    redis$HSET(keys$task_worker,   task_id,   name),
    redis$HSET(keys$task_status,   task_id,   TASK_RUNNING),
    redis$HGET(keys$task_complete, task_id),
    redis$HGET(keys$task_expr,     task_id))
  list(task = bin_to_object(dat[[7]]), key_complete = dat[[6]])
}


worker_run_task_cleanup <- function(worker, task_id, status, value,
                                    key_complete) {
  keys <- worker$keys
  name <- worker$name
  log_status <- paste0("TASK_", status)
  worker$con$pipeline(
    redis$HSET(keys$task_result,    task_id,  object_to_bin(value)),
    redis$HSET(keys$task_status,    task_id,  status),
    redis$HSET(keys$worker_status,  name,     WORKER_IDLE),
    redis$HDEL(keys$worker_task,    name),
    if (!is.null(key_complete)) {
      redis$RPUSH(key_complete, task_id)
    },
    worker_log(redis, keys, log_status, task_id))
  invisible()
}
