rrq_clean <- function(con, queue_name, delete = 0, workers_stop = FALSE) {
  keys <- rrq_keys(queue_name)
  if (!identical(workers_stop, FALSE)) {
    type <- if (isTRUE(workers_stop)) "message" else workers_stop
    workers_stop(con, keys, type = type, timeout = .1)
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

rrq_find_workers <- function(con) {
  keys <- redux::scan_find(con, "rrq:*:workers:info")
  f <- function(k) {
    d <- redux::from_redis_hash(con, k, f = as.list)
    lapply(d, unserialize)
  }
  res <- lapply(keys, f)
  names(res) <- sub("^rrq:([^:]+):.*", "\\1", keys)
}
