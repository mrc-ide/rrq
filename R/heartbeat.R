heartbeat_time_remaining <- function(controller) {
  con <- controller$con
  worker_ids <- rrq_worker_list(controller = controller)
  info <- rrq_worker_info(worker_ids, controller = controller)
  ttl <- function(key) {
    if (is.null(key)) Inf else con$PTTL(key)
  }
  vnapply(info, function(x) ttl(x$heartbeat_key))
}


cleanup_orphans <- function(controller, time) {
  worker_id <- names(time)[time < 0]

  if (length(worker_id) == 0L) {
    return(invisible(NULL))
  }

  con <- controller$con
  keys <- controller$keys
  store <- controller$store

  message(sprintf(
    "Lost %s %s:\n%s",
    length(worker_id), ngettext(length(worker_id), "worker", "workers"),
    paste0("  - ", worker_id, collapse = "\n")))

  task_ids <- rrq_worker_task_id(worker_id, controller = controller)
  i <- !is.na(task_ids)

  if (sum(i) > 0) {
    message(sprintf(
      "Orphaning %s %s:\n%s",
      length(task_ids), ngettext(sum(i), "task", "tasks"),
      paste0("  - ", task_ids, collapse = "\n")))
    run_task_cleanup_failure(con, keys, store, task_ids, TASK_DIED, NULL)
  }

  con$HMSET(keys$worker_status, worker_id, rep(WORKER_LOST, length(worker_id)))
  con$SREM(keys$worker_id, worker_id)

  invisible(task_ids)
}
