worker_run_task <- function(worker, task_id) {
  worker_run_task_start(worker, task_id)
  task_status <- worker_run_task_rrq(worker, task_id)
  worker_run_task_cleanup(worker, task_id, task_status)
}


worker_run_task_start <- function(worker, task_id) {
  keys <- worker$keys
  name <- worker$name
  worker$log("TASK_START", task_id)
  worker$con$pipeline(
    redis$HSET(keys$worker_status, name,      WORKER_BUSY),
    redis$HSET(keys$worker_task,   name,      task_id),
    redis$HSET(keys$task_worker,   task_id,   name),
    redis$HSET(keys$task_status,   task_id,   TASK_RUNNING))
}


worker_run_task_rrq <- function(worker, task_id) {
  keys <- worker$keys
  con <- worker$con

  dat <- bin_to_object(con$HGET(keys$task_expr, task_id))
  e <- expression_restore_locals(dat, worker$envir, worker$db)

  res <- expression_eval_safely(dat$expr, e)
  value <- res$value

  task_status <- if (res$success) TASK_COMPLETE else TASK_ERROR
  con$pipeline(
    redis$HSET(keys$task_result, task_id, object_to_bin(value)),
    redis$HSET(keys$task_status, task_id, task_status))

  task_status
}


worker_run_task_cleanup <- function(worker, task_id, task_status) {
  con <- worker$con
  keys <- worker$keys
  key_complete <- con$HGET(keys$task_complete, task_id)
  name <- worker$name

  ## TODO: for interrupted tasks, I don't know that we should
  ## write to the key_complete set; at the same time _not_ doing
  ## that requires that we can gracefully (and automatically)
  ## handle the restarts.
  con$pipeline(
    redis$HSET(keys$task_status,    task_id,  task_status),
    redis$HSET(keys$worker_status,  name,     WORKER_IDLE),
    redis$HDEL(keys$worker_task,    name),
    if (!is.null(key_complete)) {
      redis$RPUSH(key_complete, task_id)
    })

  worker$log(paste0("TASK_", task_status), task_id)
}
