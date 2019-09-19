worker_run_task <- function(worker, task_id, rrq) {
  worker_run_task_start(worker, task_id)
  if (rrq) {
    task_status <- worker_run_task_rrq(worker, task_id)
  } else {
    task_status <- worker_run_task_context(worker, task_id)
  }
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
  e <- context::restore_locals(dat, worker$envir, worker$db)

  cl <- c("rrq_task_error", "try-error")
  res <- context:::eval_safely(dat$expr, e, cl, 3L)
  value <- res$value

  task_status <- if (res$success) TASK_COMPLETE else TASK_ERROR
  con$pipeline(
    redis$HSET(keys$task_result, task_id, object_to_bin(value)),
    redis$HSET(keys$task_status, task_id, task_status))

  task_status
}


worker_run_task_context <- function(worker, task_id) {
  if (is.null(worker$log_path)) {
    log_file <- NULL
  } else {
    log_path <- file.path(worker$log_path, paste0(task_id, ".log"))
    worker$db$set(task_id, log_path, "log_path")
    log_file <- file.path(worker$context$root$path, log_path)
  }
  value <- context::task_run(task_id, worker$context, log_file)
  task_status <- if (inherits(value, "context_task_error"))
              TASK_ERROR else TASK_COMPLETE
  task_status
}


worker_run_task_cleanup <- function(worker, task_id, task_status) {
  con <- worker$con
  keys <- worker$keys
  key_complete <- con$HGET(keys$task_complete, task_id)
  name <- worker$name

  ## TODO: I should enforce a max size policy here.  So if the
  ## return value is too large (say more than a few kb) we can
  ## refuse to write it to the db but instead route it through the
  ## context db.  That policy can be set by the db pretty easily
  ## actually.
  ##
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
