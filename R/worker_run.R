worker_run_task <- function(worker, private, task_id) {
  task <- worker_run_task_start(worker, private, task_id)
  if (task$separate_process) {
    res <- worker_run_task_separate_process(task, worker)
  } else {
    res <- worker_run_task_local(task, worker, private)
  }
  task_status <- res$status
  worker_queue_dependencies(worker, task_id, task_status)
  worker_run_task_cleanup(worker, private, task_status, res$value)
}


worker_run_task_result <- function(result) {
  list(value = result$value,
       status = if (result$success) TASK_COMPLETE else TASK_ERROR)
}


worker_run_task_local <- function(task, worker, private) {
  e <- expression_restore_locals(task, private$envir, private$store)
  result <- withCallingHandlers(
    expression_eval_safely(task$expr, e),
    progress = function(e)
      task_progress_update(unclass(e), worker, FALSE))
  worker_run_task_result(result)
}


worker_run_task_separate_process <- function(task, worker) {
  con <- worker$con
  keys <- worker$keys
  redis_config <- con$config()
  queue_id <- keys$queue_id
  worker_id <- worker$name
  task_id <- task$id
  key_cancel <- keys$task_cancel

  worker$log("REMOTE", task_id)
  px <- callr::r_bg(function(redis_config, queue_id, worker_id, task_id)
    remote_run_task(redis_config, queue_id, worker_id, task_id),
    list(redis_config, queue_id, worker_id, task_id),
    package = "rrq", supervise = TRUE)

  ## Will make configurable in mrc-2357:
  timeout_poll <- 1
  timeout_die <- 2

  timeout_task <- con$HGET(keys$task_timeout, task_id)
  if (!is.null(timeout_task)) {
    timeout_task <- Sys.time() + as.numeric(timeout_task)
  }

  task_terminate <- function(log, status) {
    worker$log(log)
    px$signal(tools::SIGTERM)
    wait_timeout("Waiting for task to stop", timeout_die, px$is_alive)
    list(value = worker_task_failed(status), status = status)
  }

  repeat {
    result <- process_poll(px, timeout_poll)
    if (!px$is_alive() && result == "ready") {
      ## The only failure here I have identified is that if the task
      ## dies or is killed then we get an error of class
      ## callr_status_error saying something:
      ##
      ## callr subprocess failed: could not start R, exited with non-zero
      ##   status, has crashed or was killed
      ##
      ## A look through the callr sources suggests this is correct.
      return(tryCatch(
        px$get_result(),
        error = function(e)
          list(value = worker_task_failed(TASK_DIED), status = TASK_DIED)))
    }
    if (!is.null(con$HGET(key_cancel, task_id))) {
      return(task_terminate("CANCEL", TASK_CANCELLED))
    }
    if (!is.null(timeout_task) && Sys.time() > timeout_task) {
      return(task_terminate("TIMEOUT", TASK_TIMEOUT))
    }
  }
}


remote_run_task <- function(redis_config, queue_id, worker_id, task_id) {
  worker_name <- sprintf("%s_%s", worker_id, ids::random_id(bytes = 4))
  con <- redux::hiredis(config = redis_config)
  worker <- rrq_worker$new(queue_id, con, worker_name = worker_name,
                           register = FALSE)
  task <- bin_to_object(con$HGET(worker$keys$task_expr, task_id))

  ## Ensures that the worker and task will be found by
  ## rrq_task_progress_update
  cache$active_worker <- worker
  worker$active_task <- list(task_id = task_id)

  ## A hack for now, later we'll move this into a method on the object?
  private <- worker[[".__enclos_env__"]]$private

  worker_run_task_local(task, worker, private)
}


worker_run_task_start <- function(worker, private, task_id) {
  keys <- worker$keys
  name <- worker$name
  dat <- worker$con$pipeline(
    worker_log(redis, keys, "TASK_START", task_id, private$verbose),
    redis$HSET(keys$worker_status, name,      WORKER_BUSY),
    redis$HSET(keys$worker_task,   name,      task_id),
    redis$HSET(keys$task_worker,   task_id,   name),
    redis$HSET(keys$task_status,   task_id,   TASK_RUNNING),
    redis$HGET(keys$task_complete, task_id),
    redis$HGET(keys$task_local,    task_id),
    redis$HGET(keys$task_expr,     task_id),
    redis$HGET(keys$task_cancel,   task_id))

  ## This holds the bits of worker state we might need to refer to
  ## later for a running task:
  worker$active_task <- list(task_id = task_id, key_complete = dat[[6]])

  ## And this holds the data used in worker_run_task_to actually run
  ## the task
  ret <- bin_to_object(dat[[8]])
  ret$separate_process <- dat[[7]] == "FALSE" # NOTE: not a coersion
  ret$id <- task_id
  ret
}


worker_run_task_cleanup <- function(worker, private, status, value) {
  task <- worker$active_task
  task_id <- task$task_id
  key_complete <- task$key_complete

  keys <- worker$keys
  name <- worker$name
  log_status <- paste0("TASK_", status)

  task_result <- private$store$set(value, task_id)

  worker$con$pipeline(
    redis$HSET(keys$task_result,    task_id,  task_result),
    redis$HSET(keys$task_status,    task_id,  status),
    redis$HSET(keys$worker_status,  name,     WORKER_IDLE),
    redis$HDEL(keys$worker_task,    name),
    redis$RPUSH(rrq_key_task_complete(keys$queue_id, task_id), task_id),
    if (!is.null(key_complete)) {
      redis$RPUSH(key_complete, task_id)
    },
    worker_log(redis, keys, log_status, task_id, private$verbose))

  worker$active_task <- NULL
  invisible()
}


process_poll <- function(px, timeout) {
  processx::poll(list(px$get_poll_connection()), timeout * 1000)[[1L]]
}


worker_task_failed <- function(reason) {
  ret <- list(message = sprintf("Task not successful: %s", reason),
              reason = reason)
  class(ret) <- c("rrq_task_error", "error", "condition")
  ret
}
