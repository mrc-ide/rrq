## Find rrq tasks that have gone awol;

## TODO: merge into support.R?

find_rrq_workers <- function(con) {
  keys <- redux::scan_find(con, "rrq:*:workers:info")
  f <- function(k) {
    d <- redux::from_redis_hash(con, k, f=as.list)
    lapply(d, unserialize)
  }
  res <- lapply(keys, f)
  names(res) <- sub("^rrq:([^:]+):.*", "\\1", keys)
}
