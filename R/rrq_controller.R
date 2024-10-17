task_submit <- function(controller, task_ids, dat, queue,
                        separate_process, timeout, depends_on = NULL) {
  con <- controller$con
  keys <- controller$keys

  n <- length(dat)
  queue <- queue %||% QUEUE_DEFAULT
  key_queue <- rrq_key_queue(keys$queue_id, queue)

  local <- if (separate_process) "FALSE" else "TRUE"

  if (!is.null(timeout)) {
    if (!separate_process) {
      stop("Can't set timeout as 'separate_process' is FALSE")
    }
    timeout <- list(
      redis$HMSET(keys$task_timeout, task_ids, as.character(timeout)))
  }

  depends_up_original_keys <- rrq_key_task_depends_up_original(
    keys$queue_id, task_ids)
  depends_up_keys <- rrq_key_task_depends_up(keys$queue_id, task_ids)
  depends_down_keys <- rrq_key_task_depends_down(keys$queue_id, depends_on)

  cmds <- list()

  time <- timestamp()
  cmds <- c(
    cmds,
    list(
      redis$HMSET(keys$task_expr, task_ids, dat),
      redis$HMSET(keys$task_status, task_ids, rep_len(TASK_PENDING, n)),
      redis$HMSET(keys$task_queue, task_ids, rep_len(queue, n)),
      redis$HMSET(keys$task_local, task_ids, rep_len(local, n)),
      redis$HMSET(keys$task_time_submit, task_ids, rep_len(time, n))),
    timeout)
  if (length(depends_on) > 0) {
    cmds <- c(
      cmds,
      list(
        status = redis$HMGET(keys$task_status, depends_on),
        redis$HMSET(keys$task_status, task_ids, rep_len(TASK_DEFERRED, n))),
      lapply(depends_up_original_keys, redis$SADD, depends_on),
      lapply(depends_up_keys, redis$SADD, depends_on),
      lapply(depends_down_keys, redis$SADD, task_ids))
  } else {
    cmds <- c(cmds, list(redis$RPUSH(key_queue, task_ids)))
  }
  response <- con$pipeline(.commands = cmds)

  ## If any dependencies will never be satisfied then cleanup and error
  ## We do it this way around i.e. queue then check status of dependencies to
  ## avoid a race condition. If we were to check status of dependencies
  ## then queue we could get into condition where e.g.
  ## 1. Run report B which depends on report A
  ## 2. Check status of A and it is running
  ## 3. Add B to the queue
  ## In the time between 2 and 3 A could have finished and failed meaning that
  ## the dependency of B will never be satisfied and it will never be run.
  if (any(response$status %in% TASK$terminal_fail)) {
    run_task_cleanup_failure(controller, task_ids, TASK_IMPOSSIBLE, NULL)
    incomplete <- response$status[response$status %in% TASK$terminal_fail]
    names(incomplete) <- depends_on[response$status %in% TASK$terminal_fail]
    stop(sprintf("Failed to queue as dependent tasks failed:\n%s",
                 paste0(paste0(names(incomplete), ": ", incomplete),
                        collapse = ", ")))
  }

  complete <- depends_on[response$status == TASK_COMPLETE]
  for (dep_id in complete) {
    queue_dependencies(controller, dep_id, task_ids)
  }

  task_ids
}


controller_info <- function() {
  list(hostname = hostname(),
       pid = process_id(),
       username = username(),
       time = Sys.time())
}

worker_naturalsort <- function(x) {
  re <- "^(.*)_(\\d+)$"
  root <- sub(re, "\\1", x)
  i <- grepl(re, x)
  idx <- numeric(length(x))
  idx[i] <- as.integer(sub(re, "\\2", x[i]))
  x[order(root, idx)]
}


rrq_object_store <- function(con, keys, offload_path) {
  config <- rrq_configure_read(con, keys)

  offload_path <- offload_path %||% config$offload_path
  if (is.null(offload_path)) {
    offload <- object_store_offload_null$new()
  } else {
    offload <- object_store_offload_disk$new(offload_path)
  }
  object_store$new(con, keys$object_store,
                   config$store_max_size, offload)
}


verify_dependencies_exist <- function(controller, depends_on) {
  if (!is.null(depends_on)) {
    dependencies_exist <- rrq_task_exists(depends_on, named = TRUE,
                                          controller = controller)
    if (!all(dependencies_exist)) {
      missing <- names(dependencies_exist[!dependencies_exist])
      error_msg <- ngettext(
        length(missing),
        "Failed to queue as dependency %s does not exist.",
        "Failed to queue as dependencies %s do not exist.")
      stop(sprintf(error_msg, paste0(missing, collapse = ", ")))
    }
  }
  invisible(TRUE)
}
