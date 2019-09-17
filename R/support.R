rrq_clean <- function(con, queue_name, delete = 0, worker_stop = FALSE) {
  keys <- rrq_keys(queue_name)
  if (!identical(worker_stop, FALSE)) {
    type <- if (isTRUE(worker_stop)) "message" else worker_stop
    worker_stop(con, keys, type = type, timeout = .1, progress = FALSE)
  }
  pat <- sprintf("rrq:%s:*", queue_name)
  if (isTRUE(delete) || delete == 0) {
    redux::scan_del(con, pat)
  } else if (delete > 0) {
    scan_expire(con, pat, delete)
  } else {
    stop("Invalid value for delete")
  }
}
