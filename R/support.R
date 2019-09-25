rrq_clean <- function(con, queue_name, delete = 0, worker_stop = FALSE,
                      worker_stop_timeout = 0) {
  keys <- rrq_keys(queue_name)
  if (!identical(worker_stop, FALSE)) {
    type <- if (isTRUE(worker_stop)) "message" else worker_stop
    worker_stop(con, keys, type = type, timeout = worker_stop_timeout,
                progress = FALSE)
  }
  pat <- sprintf("%s:*", queue_name)
  delete_keys(con, pat, delete)
}
