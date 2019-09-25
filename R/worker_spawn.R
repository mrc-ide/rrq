##' Spawn a worker in the background
##'
##' Spawning multiple workers.  If \code{n} is greater than one,
##' multiple workers will be spawned.  This happens in parallel so it
##' does not take n times longer than spawing a single worker.
##'
##' Beware that signals like Ctrl-C passed to \emph{this} R instance
##' can still propagate to the child processes and can result in them
##' dying unexpectedly.  It is probably safer to start processes in a
##' completely separate session.
##'
##' @title Spawn a worker
##'
##' @param obj An \code{rrq_controller} object
##'
##' @param n Number of workers to spawn
##'
##' @param logdir Path of a log directory to write the worker process
##'   log to, interpreted relative to the current working directory
##'   (not affected by \code{path} below).
##'
##' @param timeout Time to wait for workers to appear
##'
##' @param worker_config Name of the configuration to use.  By default
##'   the \code{"localhost"} configuration is used
##'
##' @param worker_name_base Optional base to construct the worker
##'   names from.  If omitted a random name will be used.
##'
##' @param path Path to start the worker in.  By default workers will
##'   start in the current working directory, but you can start them
##'   elsewhere by providing a path here.  If the path does not exist,
##'   an error will be thrown (all workers will start in the same
##'   working directory).
##'
##' @param time_poll Polling period (in seconds) while waiting for
##'   workers to come up.  Must be an integer, at least 1.
##'
##' @param progress Show a progress bar while waiting for workers
##'   (when \code{timeout} is at least 0)
##'
##' @export
worker_spawn <- function(obj, n = 1, logdir = NULL,
                          timeout = 600, worker_config = "localhost",
                          worker_name_base = NULL, path = NULL,
                          time_poll = 1, progress = NULL) {
  assert_is(obj, "rrq_controller")
  if (!(worker_config %in% obj$worker_config_list())) {
    stop(sprintf("Invalid rrq worker configuration key '%s'", worker_config))
  }
  if (!is.null(path)) {
    owd <- setwd(path)
    on.exit(setwd(owd))
  }

  rrq_worker <- write_rrq_worker(versioned = TRUE)
  env <- paste0("RLIBS=", paste(.libPaths(), collapse = ":"),
                ' R_TESTS=""')
  worker_name_base <- worker_name_base %||% ids::adjective_animal()
  worker_names <- sprintf("%s_%d", worker_name_base, seq_len(n))
  key_alive <- rrq_expect_worker(obj, worker_names)

  ## log files for the process
  logdir <- logdir %||% tempfile()
  dir.create(logdir, FALSE, TRUE)
  logfile <- file.path(logdir, worker_names)

  queue_id <- obj$keys$queue_name

  message(sprintf("Spawning %d %s with prefix %s",
                  n, ngettext(n, "worker", "workers"), worker_name_base))

  for (i in seq_len(n)) {
    args <- c(queue_id,
              "--config", worker_config,
              "--name", worker_names[[i]],
              "--key-alive", key_alive)
    system2(rrq_worker, args, env = env, wait = FALSE,
            stdout = logfile[[i]], stderr = logfile[[i]])
  }

  if (timeout > 0) {
    worker_wait(obj, key_alive, timeout, time_poll, progress, logdir)
  } else {
    list(key_alive = key_alive,
         names = worker_names)
  }
}

##' @export
##' @rdname worker_spawn
##' @param key_alive A key name (generated from
##'   \code{\link{rrq_expect_worker}} or \code{worker_spawn})
worker_wait <- function(obj, key_alive, timeout = 600, time_poll = 1,
                        progress = NULL, logdir = NULL) {
  con <- obj$con
  keys <- obj$keys

  bin <- con$HGET(keys$worker_expect, key_alive)
  if (is.null(bin)) {
    stop("No workers expected on that key")
  }
  expected <- bin_to_object(bin)

  n <- length(expected)
  p <- queuer::progress_timeout(total = n, show = progress, timeout = timeout)
  time_poll <- min(time_poll, timeout)
  ret <- rep.int(NA_character_, n)

  ## Previously found workers:
  previous <- intersect(expected, worker_list(con, keys))
  if (length(previous) > 0L) {
    ret[seq_along(previous)] <- previous
    p(length(previous))
    con$LTRIM(key_alive, length(previous), -1)
  }

  i <- sum(!is.na(ret)) + 1L
  while (any(is.na(ret))) {
    x <- con$BLPOP(key_alive, time_poll)
    if (is.null(x)) {
      if (p(0)) {
        n_msg <- sum(is.na(ret))
        message(sprintf("%d / %d workers not identified in time", n_msg, n))
        missing <- setdiff(expected, ret[!is.na(ret)])
        logs <- worker_read_failed_logs(logdir, missing)
        worker_print_failed_logs(logs)
        stop(sprintf("Not all workers recovered (key_alive: %s)", key_alive))
      }
    } else {
      ret[[i]] <- x[[2]]
      i <- i + 1L
      p(1)
    }
  }
  ret
}

##' Register that workers are expected.  This generates a key that one
##' or more workers will write to when they start up (as used by
##' \code{worker_spawn}).
##' @title Registed expected workers
##' @param obj A rrq_controller object
##' @param names Names of expected workers
##' @export
rrq_expect_worker <- function(obj, names) {
  key_alive <- rrq_key_worker_alive(obj$keys$queue_name)
  obj$con$HSET(obj$keys$worker_expect, key_alive, object_to_bin(names))
  key_alive
}

## These bits exist in separate functions to keep the bits above
## relatively straightforward and to help with (eventual) unit
## testing.
worker_read_failed_logs <- function(logdir, missing) {
  log_file <- file.path(logdir, missing)
  i <- file.exists(log_file)
  set_names(lapply(log_file[i], readLines), missing[i])
}

worker_print_failed_logs <- function(logs) {
  if (is.null(logs)) {
    cat("Logging not enabled for these workers\n")
  } else {
    ## A bit cheeky, but simplifies getting a nice bit of printing done:
    message(sprintf("Log files recovered for %d %s",
                    length(logs), ngettext(length(logs), "worker", "workers")))
    dat <- list(str = names(logs), body = unname(logs))
    class(dat) <- "context_log"
    print(dat)
  }
}
