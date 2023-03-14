worker_run_task <- function(worker, private, task_id) {
  task <- worker_run_task_start(worker, private, task_id)
  if (task$separate_process) {
    res <- worker_run_task_separate_process(task, worker, private)
  } else {
    res <- worker_run_task_local(task, worker, private)
  }
  task_status <- res$status
  dependent_ids <- get_dependent_ids(private$con, private$keys$queue_id,
                                     task_id)
  if (length(dependent_ids) > 0) {
    if (identical(task_status, TASK_COMPLETE)) {
      queue_dependencies(private$con, private$keys, task_id, dependent_ids)
    } else {
      cancel_dependencies(private$con, private$keys, private$store, task_id)
    }
  }
  worker_run_task_cleanup(worker, private, task_status, res$value)
}


worker_run_task_local <- function(task, worker, private) {
  e <- expression_restore_locals(task, private$envir, private$store)
  result <- withCallingHandlers(
    expression_eval_safely(task$expr, e),
    progress = function(e) worker$progress(unclass(e), FALSE))
  if (result$success) {
    list(value = result$value,
         status = TASK_COMPLETE)
  } else {
    list(value = rrq_task_error(result$value, TASK_ERROR,
                                private$keys$queue_id, task$id),
         status = TASK_ERROR)
  }
}


worker_run_task_separate_process <- function(task, worker, private) {
  con <- private$con
  keys <- private$keys
  redis_config <- con$config()
  queue_id <- keys$queue_id
  worker_id <- worker$name
  task_id <- task$id
  key_cancel <- keys$task_cancel
  timeout_poll <- private$timeout_poll
  timeout_die <- private$timeout_die

  worker$log("REMOTE", task_id)
  px <- callr::r_bg(
    function(redis_config, queue_id, worker_id, task_id) {
      remote_run_task(redis_config, queue_id, worker_id, task_id)
    },
    list(redis_config, queue_id, worker_id, task_id),
    package = "rrq",
    supervise = TRUE)
  worker$log("REMOTE_PID", px$get_pid())

  con$HSET(keys$task_pid, task_id, px$get_pid())

  timeout_task <- con$HGET(keys$task_timeout, task_id)
  if (!is.null(timeout_task)) {
    timeout_task <- Sys.time() + as.numeric(timeout_task)
  }

  task_terminate <- function(log, status) {
    worker$log(log)
    px$signal(tools::SIGTERM)
    wait_timeout("Waiting for task to stop", timeout_die, px$is_alive)
    list(value = worker_task_failed(status, queue_id, task_id),
         status = status)
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
        error = function(e) {
          list(value = worker_task_failed(TASK_DIED, queue_id, task_id),
               status = TASK_DIED)
        }))
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
  worker$task_eval(task_id)
}


worker_run_task_start <- function(worker, private, task_id) {
  keys <- private$keys
  name <- worker$name
  dat <- private$con$pipeline(
    worker_log(redis, keys, "TASK_START", task_id, private$verbose),
    redis$HSET(keys$worker_status,   name,    WORKER_BUSY),
    redis$HSET(keys$worker_task,     name,    task_id),
    redis$HSET(keys$task_worker,     task_id, name),
    redis$HSET(keys$task_status,     task_id, TASK_RUNNING),
    redis$HSET(keys$task_time_start, task_id, timestamp()),
    redis$HGET(keys$task_complete,   task_id), # dat[[7]]
    redis$HGET(keys$task_local,      task_id), # dat[[8]]
    redis$HGET(keys$task_expr,       task_id), # dat[[9]]
    redis$HGET(keys$task_cancel,     task_id)) # unused? (TODO)

  ## TODO: we should save the root task in the db, then subsequent
  ## re-queues are easy. We can also set the leaf task at the same
  ## time and make follows very quick?
  ##
  ## Pull the data out of this bulk response, including following the
  ## redirect if needed.
  if (is.character(dat[[9]])) {
    task_id_root <- dat[[9]]
    dat[7:9] <-
      private$con$pipeline(
        redis$HGET(keys$task_complete, task_id_root), # dat[[7]]
        redis$HGET(keys$task_local,    task_id_root), # dat[[8]]
        redis$HGET(keys$task_expr,     task_id_root)) # dat[[9]]
  }

  ## This holds the bits of worker state we might need to refer to
  ## later for a running task:
  private$active_task <- list(task_id = task_id, key_complete = dat[[7]])

  ## And this holds the data used in worker_run_task_to actually run
  ## the task
  ret <- bin_to_object(dat[[9]])
  ret$separate_process <- dat[[8]] == "FALSE" # NOTE: not a coersion
  ret$id <- task_id
  ret
}


worker_run_task_cleanup <- function(worker, private, status, value) {
  task <- private$active_task
  key_complete <- task$key_complete

  keys <- private$keys
  name <- worker$name
  log_status <- paste0("TASK_", status)

  run_task_cleanup(private$con, keys, private$store, task$task_id, status,
                   value)

  private$con$pipeline(
    redis$HSET(keys$worker_status,      name,    WORKER_IDLE),
    redis$HDEL(keys$worker_task,        name),
    worker_log(redis, keys, log_status, task$task_id, private$verbose))

  private$active_task <- NULL
  invisible()
}


process_poll <- function(px, timeout) {
  processx::poll(list(px$get_poll_connection()), timeout * 1000)[[1L]]
}


worker_task_failed <- function(status, queue_id, task_id) {
  e <- structure(
    list(message = sprintf("Task not successful: %s", status)),
    class = c("error", "condition"))
  rrq_task_error(e, status, queue_id, task_id)
}


rrq_task_error <- function(e, status, queue_id, task_id) {
  e$queue_id <- queue_id
  e$task_id <- task_id
  e$status <- status
  class(e) <- c("rrq_task_error", class(e))
  e
}
