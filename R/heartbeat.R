heartbeat <- function(con, keys, period, verbose) {
  if (!is.null(period)) {
    key <- keys$worker_heartbeat
    worker_log(con, keys, "HEARTBEAT", key, verbose)
    loadNamespace("heartbeatr")
    config <- con$config()
    ret <- heartbeatr::heartbeat(key, period, config = config)
    worker_log(con, keys, "HEARTBEAT", "OK", verbose)
    ret
  }
}


worker_detect_exited <- function(obj) {
  time <- heartbeat_time_remaining(obj)
  cleanup_orphans(obj, time)
}


heartbeat_time_remaining <- function(obj) {
  info <- obj$worker_info(obj$worker_list())
  ttl <- function(key) {
    if (is.null(key)) Inf else obj$con$PTTL(key)
  }
  vnapply(info, function(x) ttl(x$heartbeat_key))
}


cleanup_orphans <- function(obj, time) {
  worker_id <- names(time)[time < 0]

  if (length(worker_id) == 0L) {
    return(invisible(NULL))
  }

  message(sprintf(
    "Lost %s %s:\n%s",
    length(worker_id), ngettext(length(worker_id), "worker", "workers"),
    paste0("  - ", worker_id, collapse = "\n")))

  task_id <- obj$worker_task_id(worker_id)
  i <- !is.na(task_id)

  if (sum(i) > 0) {
    message(sprintf(
      "Orphaning %s %s:\n%s",
      length(task_id), ngettext(sum(i), "task", "tasks"),
      paste0("  - ", task_id, collapse = "\n")))
    obj$con$HMSET(obj$keys$task_status, task_id[i],
                  rep(TASK_ORPHAN, sum(i)))
  }

  obj$con$HMSET(obj$keys$worker_status, worker_id,
                rep(WORKER_LOST, length(worker_id)))
  obj$con$SREM(obj$keys$worker_name, worker_id)

  invisible(task_id)
}
