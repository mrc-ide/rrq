## This is duplicate of collect_wait_n_poll, except that one polls a
## *single* hash and this one polls a set of hashes.  would be good to
## merge them perhaps.
##
## It's only used in one place
poll_hash_keys <- function(con, keys, field, timeout, time_poll, progress) {
  if (timeout <= 0) {
    res <- lapply(keys, con$HGET, field)
  } else {
    time_poll <- time_poll %||% 0.1
    p <- queuer::progress_timeout(length(keys),
                                  show = progress, timeout = timeout)
    ok <- logical(length(keys))
    res <- vector("list", length(keys))
    while (!all(ok)) {
      exists <- as.logical(vnapply(keys[!ok], con$HEXISTS, field))
      if (any(exists)) {
        i <- which(!ok)[exists]
        res[i] <- lapply(keys[i], con$HGET, field)
        ok[i] <- TRUE
        p(length(i))
      } else {
        if (p(0)) {
          p(clear = TRUE)
          break
        }
        Sys.sleep(time_poll)
      }
    }
  }
  names(res) <- keys
  res
}

## This is pretty cool:
scan_expire <- function(con, pattern, seconds) {
  n <- 0L
  expire <- function(keys) {
    if (length(keys) > 0L) {
      n <<- n + sum(viapply(keys, con$EXPIRE, seconds) > 0)
    }
  }
  redux::scan_apply(con, expire, pattern)
  n
}
