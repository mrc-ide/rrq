poll_hash_keys <- function(con, keys, field, wait, every = 0.05) {
  if (wait <= 0) {
    res <- lapply(keys, con$HGET, field)
  } else {
    ## TODO: I have a generic timeout thing in queuer (time_checker)
    timeout <- as.difftime(wait, units = "secs")
    t0 <- Sys.time()
    ok <- logical(length(keys))
    res <- vector("list", length(keys))
    while (Sys.time() - t0 < timeout) {
      exists <- as.logical(vnapply(keys[!ok], con$HEXISTS, field))
      if (any(exists)) {
        i <- which(!ok)[exists]
        res[i] <- lapply(keys[i], con$HGET, field)
        ok[i] <- TRUE
        if (all(ok)) {
          break
        }
      }
      ## This should not be called on the last way through...
      Sys.sleep(every)
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
