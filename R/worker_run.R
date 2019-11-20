worker_run_task <- function(worker, task_id) {
  task <- worker_run_task_start(worker, task_id)
  e <- expression_restore_locals(task, worker$envir, worker$db)
  res <- withCallingHandlers(
    expression_eval_safely(task$expr, e),
    progress = function(e) task_progress_update(e$message, worker, FALSE))
  task_status <- if (res$success) TASK_COMPLETE else TASK_ERROR
  worker_run_task_cleanup(worker, task_status, res$value)
}


worker_run_task_start <- function(worker, task_id) {
  keys <- worker$keys
  name <- worker$name
  dat <- worker$con$pipeline(
    worker_log(redis, keys, "TASK_START", task_id, worker$verbose),
    redis$HSET(keys$worker_status, name,      WORKER_BUSY),
    redis$HSET(keys$worker_task,   name,      task_id),
    redis$HSET(keys$task_worker,   task_id,   name),
    redis$HSET(keys$task_status,   task_id,   TASK_RUNNING),
    redis$HGET(keys$task_complete, task_id),
    redis$HGET(keys$task_expr,     task_id))

  ## This holds the bits of worker state we might need to refer to
  ## later for a running task:
  worker$active_task <- list(task_id = task_id, key_complete = dat[[6]])

  ## And this holds the data used in worker_run_task_to actually run
  ## the task
  bin_to_object(dat[[7]])
}

worker_run_task_cleanup <- function(worker, status, value) {
  task <- worker$active_task
  task_id <- task$task_id
  key_complete <- task$key_complete

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
    worker_log(redis, keys, log_status, task_id, worker$verbose))

  worker$active_task <- NULL
  invisible()
}
