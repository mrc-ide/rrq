##' Spawn a worker in the background
##'
##' Spawning multiple workers.  If \code{n} is greater than one,
##' multiple workers will be spawned.  This happens in parallel so it
##' does not take n times longer than spawning a single worker.
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
##'
##' @param timeout Time to wait for workers to appear
##'
##' @param worker_config Name of the configuration to use.  By default
##'   the \code{"localhost"} configuration is used
##'
##' @param worker_id_base Optional base to construct the worker ids
##'   from.  If omitted a random base will be used. Actual ids will be
##'   created but appending integers to this base.
##'
##' @param time_poll Polling period (in seconds) while waiting for
##'   workers to come up.  Must be an integer, at least 1.
##'
##' @param progress Show a progress bar while waiting for workers
##'   (when \code{timeout} is at least 0)
##'
##' @export
rrq_worker_spawn <- function(obj, n = 1, logdir = NULL,
                             timeout = 600, worker_config = "localhost",
                             worker_id_base = NULL,
                             time_poll = 1, progress = NULL) {
  assert_is(obj, "rrq_controller")
  if (!(worker_config %in% obj$worker_config_list())) {
    stop(sprintf("Invalid rrq worker configuration key '%s'", worker_config))
  }

  rrq_worker <- rrq_worker_script(tempfile(), versioned = TRUE)
  env <- paste0("RLIBS=", paste(.libPaths(), collapse = ":"),
                ' R_TESTS=""')
  worker_id_base <- worker_id_base %||% ids::adjective_animal()
  worker_ids <- sprintf("%s_%d", worker_id_base, seq_len(n))
  key_alive <- rrq_expect_worker(obj, worker_ids)

  ## log files for the process
  logdir <- logdir %||% tempfile()
  dir.create(logdir, FALSE, TRUE)
  logfile <- file.path(logdir, worker_ids)

  message(sprintf("Spawning %d %s with prefix %s",
                  n, ngettext(n, "worker", "workers"), worker_id_base))

  keys <- rrq_keys(obj$queue_id)
  obj$con$HMSET(keys$worker_process, worker_ids, logfile)

  for (i in seq_len(n)) {
    args <- c(obj$queue_id,
              "--config", worker_config,
              "--id", worker_ids[[i]],
              "--key-alive", key_alive)
    system2(rrq_worker, args, env = env, wait = FALSE,
            stdout = logfile[[i]], stderr = logfile[[i]])
  }

  if (timeout > 0) {
    rrq_worker_wait(obj, key_alive, timeout, time_poll, progress)
  } else {
    list(key_alive = key_alive,
         ids = worker_ids)
  }
}

##' @export
##' @rdname rrq_worker_spawn
##' @param key_alive A key name (generated from
##'   \code{\link{rrq_expect_worker}} or \code{rrq_worker_spawn})
rrq_worker_wait <- function(obj, key_alive, timeout = 600, time_poll = 1,
                            progress = NULL) {
  assert_is(obj, "rrq_controller")
  con <- obj$con
  keys <- rrq_keys(obj$queue_id)

  bin <- con$HGET(keys$worker_expect, key_alive)
  if (is.null(bin)) {
    stop("No workers expected on that key")
  }

  expected <- bin_to_object(bin)
  previous <- worker_list(con, keys)
  done <- set_names(expected %in% previous, expected)
  if (!all(done)) {
    fetch <- function() {
      tmp <- con$BLPOP(key_alive, time_poll)
      if (!is.null(tmp)) {
        done[[tmp[[2L]]]] <<- TRUE
      }
      done
    }
    general_poll(fetch, 0, timeout, "workers", FALSE, progress)

    if (!all(done)) {
      message(sprintf("%d / %d workers not identified in time",
                      sum(!done), length(done)))
      logs <- worker_read_failed_logs(con, keys, names(done)[!done])
      worker_print_failed_logs(logs)
      stop("Not all workers recovered")
    }
  }

  expected
}

##' Register that workers are expected.  This generates a key that one
##' or more workers will write to when they start up (as used by
##' \code{rrq_worker_spawn}).
##' @title Register expected workers
##' @param obj A rrq_controller object
##' @param ids Ids of expected workers
##' @export
rrq_expect_worker <- function(obj, ids) {
  assert_is(obj, "rrq_controller")
  key_alive <- rrq_key_worker_alive(obj$queue_id)
  keys <- rrq_keys(obj$queue_id)
  obj$con$HSET(keys$worker_expect, key_alive, object_to_bin(ids))
  key_alive
}

## These bits exist in separate functions to keep the bits above
## relatively straightforward and to help with (eventual) unit
## testing.
worker_read_failed_logs <- function(con, keys, missing) {
  log_file <- from_redis_hash(con, keys$worker_process, missing)
  i <- !is.na(log_file) & file.exists(log_file)
  set_names(lapply(log_file[i], readLines), missing[i])
}

worker_print_failed_logs <- function(logs) {
  if (is.null(logs)) {
    cat("Logging not enabled for these workers\n")
  } else {
    header <- sprintf("Log files recovered for %d %s\n",
                      length(logs), ngettext(length(logs), "worker", "workers"))
    log_str <- vcapply(logs, function(x) {
      paste(sprintf("  %s\n", x), collapse = "")
    })
    txt <- c(header, sprintf("%s\n%s", names(logs), log_str))
    cat(paste(txt, collapse = "\n"))
  }
}
