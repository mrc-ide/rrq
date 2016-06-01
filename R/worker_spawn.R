##' Spawn a worker in the background
##'
##' Spawning multiple workers.  If \code{n} is greater than one,
##' multiple workers will be spawned.  This happens in parallel so it
##' does not take n times longer than spawing a single worker.
##'
##' Beware that signals like Ctrl-C passed to \emph{this} R instance
##' can still propagate to the child processes and can result in them
##' dying unexpectedly.  It is probably safer to start processes in a
##' standalone session.
##'
##' @title Spawn a worker
##' @inheritParams rrq_controller
##' @param logfile Name of a log file to write to (consider
##'   \code{tempfile()}).  If \code{n} > 1, then \code{n} log files
##'   must be provided.
##' @param n Number of workers to spawn
##' @param timeout Time to wait for the worker to appear
##' @param time_poll Period to poll for the worker (must be in
##'   seconds)
##' @param path Path to start the worker in.  By default workers will
##'   start in the current working directory, but you can start them
##'   elsewhere by providing a path here.  If the path does not exist,
##'   an error will be thrown.  If \code{n} is greater than 1, all
##'   workers will start in the same working directory.  The
##'   \code{logfile} argument will be interpreted relative to current
##'   working directory (not the worker working directory); use
##'   \code{\link{normalizePath}} to convert into an absolute path
##'   name to prevent this.
##' @export
worker_spawn <- function(context, con, logfile, n=1, timeout=20, time_poll=1,
                         path=".") {
  rrq_worker <- system.file("rrq_worker", package="rrq")
  env <- paste0("RLIBS=", paste(.libPaths(), collapse=":"),
                'R_TESTS=""')

  assert_integer_like(time_poll)
  assert_length(logfile, n)

  key_alive <- rrq_key_worker_alive(context$id)

  args <- c("--context-root", normalizePath(context$root),
            "--context-id", context$id,
            "--redis-host", con$config()$host,
            "--redis-port", con$config()$port,
            "--key-alive", key_alive)

  dir_create(dirname(logfile))
  logfile <- file.path(normalizePath(dirname(logfile)), basename(logfile))

  code <- integer(n)
  with_wd(path, {
    for (i in seq_len(n)) {
      code[[i]] <- system2(rrq_worker, args,
                           env=env, wait=FALSE,
                           stdout=logfile[[i]], stderr=logfile[[i]])
    }
  })
  if (any(code != 0L)) {
    warning("Error launching script: worker *probably* does not exist")
  }

  ret <- rep.int(NA_character_, n)
  t0 <- Sys.time()
  timeout <- as.difftime(timeout, units="secs")

  i <- 1L
  repeat {
    x <- con$BLPOP(key_alive, time_poll)
    if (is.null(x)) {
      message(".", appendLF=FALSE)
      flush.console()
    } else {
      ret[[i]] <- x[[2]]
      if (n > 1L) {
        message(sprintf("new worker: %s (%d / %d)", x[[2]], i, n))
      } else {
        message(sprintf("new worker: %s", x[[2]]))
      }
      i <- i + 1
    }
    if (!any(is.na(ret))) {
      break
    }
    if (Sys.time() - t0 > timeout) {
      ## TODO: Better recover here.  Ideally we'd stop any workers
      ## that *are* running, and provide data from the log files.
      stop(sprintf("%d / %d workers not identified in time",
                   sum(is.na(ret)), n))
    }
  }

  ret
}
