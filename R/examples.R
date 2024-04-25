enable_examples <- function(require_queue = NULL) {
  ## We might also have an opt-in environment variable here, such as
  ## RRQ_RUN_EXAMPLES, or reference NOT_CRAN perhaps?
  ##
  ## We might also use a queue called rrq:example
  if (!redis_available()) {
    return(FALSE)
  }
  if (!is.null(require_queue)) {
    r <- rrq_controller(require_queue)
    if (length(rrq_worker_list(controller = r)) == 0) {
      return(FALSE)
    }
  }
  TRUE
}


redis_available <- function() {
  if (is.null(cache$redis_available)) {
    cache$redis_available <- test_if_redis_available()
  }
  cache$redis_available
}


test_if_redis_available <- function() {
  res <- tryCatch(
    redux::hiredis()$PING(),
    error = identity)
  !inherits(res, "error")
}
