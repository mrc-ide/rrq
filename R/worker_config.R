worker_config_save <- function(con, keys, name,
                               time_poll = NULL, timeout_idle = NULL,
                               queue = NULL,
                               heartbeat_period = NULL,
                               verbose = NULL,
                               overwrite = TRUE,
                               timeout_poll = NULL,
                               timeout_die = NULL) {
  key <- keys$worker_config
  write <- overwrite || con$HEXISTS(key, name) == 0
  if (write) {
    config <- worker_config_make(time_poll, timeout_idle,
                                 queue, heartbeat_period,
                                 verbose, timeout_poll, timeout_die)
    con$HSET(key, name, object_to_bin(config))
    invisible(config)
  } else {
    NULL
  }
}


worker_config_read <- function(con, keys, name) {
  config <- con$HGET(keys$worker_config, name)
  if (is.null(config)) {
    stop(sprintf("Invalid rrq worker configuration key '%s'", name))
  }
  bin_to_object(config)
}


worker_config_make <- function(time_poll = NULL, timeout_idle = NULL,
                               queue = NULL, heartbeat_period = NULL,
                               verbose = NULL, timeout_poll = NULL,
                               timeout_die = NULL) {
  if (!is.null(time_poll)) {
    assert_scalar_integer_like(time_poll)
  }
  if (!(is.null(timeout_idle) || identical(timeout_idle, Inf))) {
    assert_scalar_integer_like(timeout_idle)
  }
  if (!is.null(queue)) {
    assert_character(queue)
  }
  if (!is.null(heartbeat_period)) {
    assert_scalar_integer_like(heartbeat_period)
  }
  if (!is.null(verbose)) {
    assert_scalar_logical(verbose)
  } else {
    verbose <- TRUE
  }
  if (!is.null(timeout_poll)) {
    assert_scalar_integer_like(timeout_poll)
  }
  if (!is.null(timeout_die)) {
    assert_scalar_integer_like(timeout_die)
  }

  config <- list(time_poll = time_poll,
                 timeout_idle = timeout_idle,
                 queue = queue,
                 heartbeat_period = heartbeat_period,
                 verbose = verbose,
                 timeout_poll = timeout_poll,
                 timeout_die = timeout_die)
  config[!vlapply(config, is.null)]
}
