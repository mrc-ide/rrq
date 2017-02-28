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
##' @param logdir Path of a log directory to write to.  This will be
##'   interpreted relative to the context root.
##'
##' @param timeout Time to wait for workers to appear
##'
##' @param worker_config Name of the configuration to use.  By default
##'   the \code{"localhost"} configuration is used, which duplicates
##'   the redis information from the local configuration but otherwise
##'   uses the defaults.
##'
##' @param worker_name_base Optional base to construct the worker
##'   names from.  If omitted a random name will be used.
##'
##' @param path Path to start the worker in.  By default workers will
##'   start in the current working directory, but you can start them
##'   elsewhere by providing a path here.  If the path does not exist,
##'   an error will be thrown.  If \code{n} is greater than 1, all
##'   workers will start in the same working directory.  The
##'   \code{logfile} argument will be interpreted relative to current
##'   working directory (not the worker working directory); use
##'   \code{\link{normalizePath}} to convert into an absolute path
##'   name to prevent this.
##'
##' @param time_poll Polling period (in seconds) while waiting for
##'   workers to come up.  Must be an integer, at least 1.
##'
##' @param progress Show a progress bar while waiting for workers
##'   (when \code{timeout} is at least 0)
##'
##' @export
workers_spawn <- function(obj, n = 1, logdir = "worker_logs",
                          timeout = 600, worker_config = "localhost",
                          worker_name_base = NULL, path = NULL,
                          time_poll = 1, progress = NULL) {
  assert_inherits(obj, "rrq_controller")
  if (!obj$db$exists(worker_config, "worker_config")) {
    stop(sprintf("worker config '%s' does not exist", worker_config))
  }
  if (!is.null(path)) {
    stop("FIXME")
  }

  rrq_worker <- file.path(obj$context$root$path, "bin", "rrq_worker")
  env <- paste0("RLIBS=", paste(.libPaths(), collapse = ":"),
                ' R_TESTS=""')
  worker_name_base <- worker_name_base %||% ids::adjective_animal()
  worker_names <- sprintf("%s_%d", worker_name_base, seq_len(n))
  key_alive <- rrq_expect_workers(obj, worker_names)

  logdir_abs <- file.path(obj$context$root$path, logdir)
  if (!file.exists(logdir_abs)) {
    dir.create(logdir_abs, FALSE, TRUE)
  }
  obj$context$db$mset(worker_names, file.path(logdir, worker_names), "log_path")
  logfile <- file.path(logdir_abs, worker_names)

  root <- normalizePath(obj$context$root$path)
  context_id <- obj$context$id

  message(sprintf("Spawning %d %s with prefix %s",
                  n, ngettext(n, "worker", "workers"), worker_name_base))
  code <- integer(n)
  ## TODO: path is never used in the package
  ## with_wd(path, {
  for (i in seq_len(n)) {
    args <- c(root, context_id, worker_config, worker_names[[i]], key_alive)
    code[[i]] <- system2(rrq_worker, args, env = env, wait = FALSE,
                         stdout = logfile[[i]], stderr = logfile[[i]])
  }
  ## })
  if (any(code != 0L)) {
    warning("Error launching script: worker *probably* does not exist")
  }

  if (timeout > 0) {
    workers_wait(obj, key_alive, timeout, time_poll, progress)
  } else {
    worker_names
  }
}

##' @export
##' @rdname workers_spawn
##' @param key_alive A key name (generated from
##'   \code{\link{rrq_expect_workers}} or \code{workers_spawn})
workers_wait <- function(obj, key_alive, timeout = 600, time_poll = 1,
                         progress = NULL) {
  con <- obj$con
  keys <- obj$keys
  expected <- bin_to_object(con$HGET(keys$workers_expect, key_alive))
  n <- length(expected)
  p <- queuer::progress_timeout(total = n, show = progress, timeout = timeout)
  time_poll <- min(time_poll, timeout)
  ret <- rep.int(NA_character_, n)

  ## Previously found workers:
  previous <- intersect(expected, workers_list(con, keys))
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
        logs <- workers_read_failed_logs(obj$context, missing)
        workers_print_failed_logs(logs)
        stop(sprintf("Not all workers recovered (key_alive: %s)", key_alive))
      }
    } else {
      ret[[i]] <- x[[2]]
      i <- i + 1L
      p(1)
    }
  }
  obj$con$HDEL(keys$workers_expect, key_alive)
  ret
}

##' Register that workers are expected
##' @title Registed expected workers
##' @param obj A rrq_controller or worker_controller object
##' @param names Names of expected workers
##' @export
rrq_expect_workers <- function(obj, names) {
  key_alive <- rrq_key_worker_alive(obj$keys$queue_name)
  obj$con$HSET(obj$keys$workers_expect, key_alive, object_to_bin(names))
  key_alive
}

## These bits exist in separate functions to keep the bits above
## relatively straightforward and to help with (eventual) unit
## testing.
workers_read_failed_logs <- function(context, missing) {
  log_path <-
    l2c(context$db$mget(missing, "log_path", missing = NA_character_))
  i <- !is.na(log_path)
  if (any(i)) {
    log_file <- file.path(context$root$path, log_path[i])
    j <- file.exists(log_file)
    logs <- lapply(log_file, readLines)
    setNames(logs, missing[i][j])
  } else {
    NULL

  }
}

workers_print_failed_logs <- function(logs) {
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
