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


blpop <- function(con, keys, timeout, immediate) {
  if (immediate) {
    for (k in keys) {
      v <- con$LPOP(k)
      if (!is.null(v)) {
        return(list(k, v))
      }
    }
    return(NULL)
  } else {
    con$BLPOP(keys, timeout)
  }
}


delete_keys <- function(con, pat, delete) {
  if (isTRUE(delete)) {
    delete <- 0
  }
  assert_scalar_integer_like(delete)
  if (is.na(delete) || delete < 0) {
    stop("Invalid value for delete")
  }

  if (delete > 0) {
    scan_expire(con, pat, delete)
  } else {
    redux::scan_del(con, pat)
  }
}


rpush_max_length <- function(con, key, value, max_length) {
  n <- con$RPUSH(key, value)
  if (n > max_length) {
    con$LTRIM(key, -max_length, -1)
  }
}


hash_exists <- function(con, key, field, over_fields = FALSE) {
  if (length(key) == 0L || length(field) == 0L) {
    return(logical(0))
  }
  if (over_fields) {
    as.logical(viapply(field, con$HEXISTS, key = key))
  } else {
    as.logical(viapply(key, con$HEXISTS, field))
  }
}
