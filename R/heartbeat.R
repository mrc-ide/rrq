worker_heartbeat <- function(con, keys, period, verbose) {
  if (!is.null(period)) {
    is_child <- FALSE
    key <- keys$worker_heartbeat
    worker_log(con, keys, "HEARTBEAT", key, is_child, verbose)
    config <- con$config()
    ret <- rrq_heartbeat$new(key, period, config = config)
    worker_log(con, keys, "HEARTBEAT", "OK", is_child, verbose)
    ret
  }
}


worker_detect_exited <- function(con, keys, store) {
  time <- heartbeat_time_remaining(con, keys)
  cleanup_orphans(con, keys, store, time)
}


heartbeat_time_remaining <- function(con, keys) {
  worker_ids <- worker_list(con, keys)
  info <- worker_info(con, keys, worker_ids)
  ttl <- function(key) {
    if (is.null(key)) Inf else con$PTTL(key)
  }
  vnapply(info, function(x) ttl(x$heartbeat_key))
}


cleanup_orphans <- function(con, keys, store, time) {
  worker_id <- names(time)[time < 0]

  if (length(worker_id) == 0L) {
    return(invisible(NULL))
  }

  message(sprintf(
    "Lost %s %s:\n%s",
    length(worker_id), ngettext(length(worker_id), "worker", "workers"),
    paste0("  - ", worker_id, collapse = "\n")))

  task_ids <- worker_task_id(con, keys, worker_id)
  i <- !is.na(task_ids)

  if (sum(i) > 0) {
    message(sprintf(
      "Orphaning %s %s:\n%s",
      length(task_ids), ngettext(sum(i), "task", "tasks"),
      paste0("  - ", task_ids, collapse = "\n")))
    run_task_cleanup_failure(con, keys, store, task_ids, TASK_DIED, NULL)
  }

  con$HMSET(keys$worker_status, worker_id, rep(WORKER_LOST, length(worker_id)))
  con$SREM(keys$worker_name, worker_id)

  invisible(task_ids)
}
