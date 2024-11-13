worker_run_task <- function(worker, private, task_id) {
  task <- worker_run_task_start(worker, private, task_id)
  if (task$separate_process) {
    res <- worker_run_task_separate_process(task, worker, private)
  } else {
    res <- worker_run_task_local(task, worker, private)
  }

  controller <- worker$controller
  con <- controller$con
  keys <- controller$keys
  status <- res$status
  if (status == TASK_COMPLETE) {
    run_task_cleanup_success(controller, task_id, status, res$value)
  } else {
    run_task_cleanup_failure(controller, task_id, status, res$value)
  }

  con$pipeline(
    redis$HSET(keys$worker_status, worker$id, WORKER_IDLE),
    redis$HDEL(keys$worker_task,   worker$id),
    worker_log(redis, private$key_log, paste0("TASK_", status), task_id,
               private$is_child, private$verbose))

  private$active_task_id <- NULL
  invisible()
}


worker_run_task_local <- function(task, worker, private) {
  envir <- private$envir
  keys <- worker$controller$keys
  store <- worker$controller$store

  top <- rlang::current_env() # not quite right, but better than nothing
  local <- new.env(parent = emptyenv())
  local$warnings <- collector(list())

  result <- rlang::try_fetch({
    ## Soon, we'll allow a change of directory here too with this:
    ## > withr::local_dir(file.path(root$path$root, task$path))
    task <- task_load_from_store(task, store)
    if (task$type == "expr") {
      if (!is.null(task$variables)) {
        rlang::env_bind(envir, !!!task$variables)
      }
      eval(task$expr, envir)
    } else { # task$type is call
      fn <- task$fn
      args <- task$args
      if (is.null(fn$name)) {
        call <- rlang::call2(fn$value, !!!args)
      } else if (is.null(fn$namespace)) {
        envir[[fn$name]] <- fn$value
        call <- rlang::call2(fn$name, !!!args)
      } else {
        call <- rlang::call2(fn$name, !!!args, .ns = fn$namespace)
      }
      eval(call, envir)
    }
  },
  warning = function(e) {
    local$warnings$add(list(e))
    tryInvokeRestart("muffleWarning")
  },
  progress = function(e) {
    worker$progress(unclass(e), FALSE)
    rlang::zap()
  },
  error = function(e) {
    if (is.null(e$trace)) {
      e$trace <- rlang::trace_back(top)
    }
    local$error <- e
    NULL
  })

  if (is.null(local$error)) {
    list(value = result, status = TASK_COMPLETE)
  } else {
    result <- local$error
    warnings <- local$warnings$get()
    if (length(warnings) > 0) {
      result$warnings <- local$warnings$get()
    }
    list(value = rrq_task_error(result, TASK_ERROR,
                                keys$queue_id, task$id),
         status = TASK_ERROR)
  }
}


worker_run_task_separate_process <- function(task, worker, private) {
  con <- worker$controller$con
  keys <- worker$controller$keys
  redis_config <- con$config()
  queue_id <- keys$queue_id
  worker_id <- worker$id
  task_id <- task$id
  key_cancel <- keys$task_cancel
  poll_process <- private$poll_process
  timeout_process_die <- private$timeout_process_die
  logdir <- private$logdir
  offload_path <- private$offload_path

  if (is.null(logdir)) {
    logfile <- "|"
  } else {
    dir_create(logdir)
    logfile <- file.path(logdir, task_id)
    con$HSET(keys$task_logfile, task_id, logfile)
  }

  worker$log("REMOTE", task_id)
  px <- callr::r_bg(
    remote_run_task,
    list(redis_config, queue_id, worker_id, task_id, offload_path),
    package = "rrq",
    supervise = TRUE,
    stdout = logfile,
    stderr = "2>&1",
    env = c(callr::rcmd_safe_env(),
            RRQ_WORKER_ID = worker_id,
            RRQ_TASK_ID = task_id))

  con$HSET(keys$task_pid, task_id, px$get_pid())

  timeout_task <- con$HGET(keys$task_timeout, task_id)
  if (!is.null(timeout_task)) {
    timeout_task <- Sys.time() + as.numeric(timeout_task)
  }

  task_terminate <- function(log, status) {
    worker$log(log)
    px$signal(tools::SIGTERM)
    wait_timeout("Waiting for task to stop", timeout_process_die, px$is_alive)
    list(value = worker_task_failed(status, queue_id, task_id),
         status = status)
  }

  repeat {
    result <- process_poll(px, poll_process)
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


remote_run_task <- function(redis_config, queue_id, worker_id, task_id,
                            offload_path) {
  worker <- rrq_worker$new(queue_id, worker_id = worker_id, is_child = TRUE,
                           offload_path = offload_path,
                           con = redux::hiredis(config = redis_config))
  on.exit(worker$log("STOP", "OK"))
  worker$task_eval(task_id)
}


worker_run_task_start <- function(worker, private, task_id) {
  con <- worker$controller$con
  keys <- worker$controller$keys
  worker_id <- worker$id
  dat <- con$pipeline(
    worker_log(redis, private$key_log, "TASK_START", task_id,
               private$is_child, private$verbose),             # 1
    redis$HSET(keys$worker_status,   worker_id, WORKER_BUSY),  # 2
    redis$HSET(keys$worker_task,     worker_id, task_id),      # 3
    redis$HSET(keys$task_worker,     task_id,   worker_id),    # 4
    redis$HSET(keys$task_status,     task_id,   TASK_RUNNING), # 5
    redis$HSET(keys$task_time_start, task_id,   timestamp()),  # 6
    redis$HGET(keys$task_local,      task_id),                 # 7
    redis$HGET(keys$task_expr,       task_id))                 # 8

  if (is_task_redirect(dat[[8]])) {
    task_id_root <- dat[[8]]
    dat[7:8] <- con$pipeline(
      redis$HGET(keys$task_local,    task_id_root),
      redis$HGET(keys$task_expr,     task_id_root))
  }

  ## This might be used later for recording task progress
  private$active_task_id <- task_id

  ## And this holds the data used in worker_run_task_to actually run
  ## the task
  ret <- bin_to_object(dat[[8]])
  ret$separate_process <- dat[[7]] == "FALSE" # NOTE: not a coersion
  ret$id <- task_id
  ret
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
