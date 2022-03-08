worker_config_save <- function(con, keys, name,
                               time_poll = NULL, timeout = NULL,
                               queue = NULL,
                               heartbeat_period = NULL,
                               logdir = NULL,
                               verbose = NULL,
                               overwrite = TRUE) {
  key <- keys$worker_config
  write <- overwrite || con$HEXISTS(key, name) == 0
  if (write) {
    config <- worker_config_make(time_poll, timeout, queue, heartbeat_period,
                                 logdir, verbose)
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


worker_config_make <- function(time_poll = NULL, timeout = NULL, queue = NULL,
                               heartbeat_period = NULL, logdir = NULL,
                               verbose = NULL) {
  if (!is.null(time_poll)) {
    assert_scalar_integer_like(time_poll)
  }
  if (!(is.null(timeout) || identical(timeout, Inf))) {
    assert_scalar_integer_like(timeout)
  }
  if (!is.null(queue)) {
    assert_character(queue)
  }
  if (!is.null(heartbeat_period)) {
    assert_scalar_integer_like(heartbeat_period)
  }
  if (!is.null(logdir)) {
    assert_scalar_character(logdir)
  } else {
    logdir <- TRUE
  }
  if (!is.null(verbose)) {
    assert_scalar_logical(verbose)
  } else {
    verbose <- TRUE
  }

  config <- list(time_poll = time_poll,
                 timeout = timeout,
                 queue = queue,
                 heartbeat_period = heartbeat_period,
                 logdir = logdir,
                 verbose = verbose)
  config[!vlapply(config, is.null)]
}
