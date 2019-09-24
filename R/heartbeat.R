heartbeat <- function(con, key, period) {
  if (!is.null(period)) {
    rrq_log("heartbeat", key)
    loadNamespace("heartbeatr")
    config <- con$config()
    ret <- heartbeatr::heartbeat(key, period, config = config)
    rrq_log("heartbeat", "OK")
    ret
  }
}

heartbeat_time_remaining <- function(obj) {
  worker_id <- obj$worker_list()
  if (length(worker_id) > 0L) {
    key <- rrq_key_worker_heartbeat(obj$keys$queue_name, worker_id)

    ## Do _all_ this in one block for consistency I think
    status <- obj$worker_status()
    time <- vnapply(key, obj$con$PTTL)
    task_id <- obj$worker_task_id(worker_id)

    ## Turn time into seconds:
    alive <- time > 0
    time[alive] <- time[alive] / 1000
  } else {
    time <- numeric(0)
    status <- task_id <- character(0)
  }

  ret <- data.frame(worker_id = worker_id,
                    time = time,
                    status = status,
                    task_id = task_id,
                    stringsAsFactors = FALSE)
  rownames(ret) <- NULL
  ret
}

identify_orphan_tasks <- function(obj) {
  dat <- heartbeat_time_remaining(obj)
  cleanup_orphans(obj, dat)
}

cleanup_orphans <- function(obj, dat) {
  i <- dat$time < 0
  if (any(i)) {
    task_id <- dat$task_id[i]
    task_id <- task_id[!is.na(task_id)]
    worker_id <- dat$worker_id[i]
    message(sprintf(
      "Lost %s %s:\n%s",
      length(worker_id), ngettext(length(worker_id), "worker", "workers"),
      paste0("  - ", worker_id, collapse = "\n")))
    message(sprintf(
      "Orphaning %s %s:\n%s",
      length(task_id), ngettext(length(task_id), "task", "tasks"),
      paste0("  - ", task_id, collapse = "\n")))
    obj$con$HMSET(obj$keys$task_status, dat$task_id[i], TASK_ORPHAN)
    obj$con$HMSET(obj$keys$worker_status, dat$worker_id[i], WORKER_LOST)
    obj$con$SREM(obj$keys$worker_name, dat$worker_id[i])
  }
  ret <- dat[i, c("worker_id", "task_id"), drop = FALSE]
  ret
}
