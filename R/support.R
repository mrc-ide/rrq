rrq_clean <- function(con, queue_id, delete = 0, worker_stop = FALSE,
                      timeout_worker_stop = 0) {
  keys <- rrq_keys(queue_id)
  if (!identical(worker_stop, FALSE)) {
    type <- if (isTRUE(worker_stop)) "message" else worker_stop
    worker_stop(con, keys, type = type, timeout = timeout_worker_stop,
                progress = FALSE)
  }
  pat <- sprintf("%s:*", queue_id)
  delete_keys(con, pat, delete)
}
