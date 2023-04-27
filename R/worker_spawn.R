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
##' @param timeout Time to wait for workers to appear. If 0 then we
##'   don't wait for workers to appear (you can run the `wait_alive`
##'   method of the returned object to run this test manually)
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
##' @return An `rrq_worker_manager` object with fields:
##'
##' * `id`: the ids of the spawned workers
##' * `wait_alive`: a method to wait for workers to come alive
##' * `stop`: a method to stop workers
##' * `kill`: a method to kill workers abruptly by sending a signal
##' * `is_alive`: a method that checks if a worker is currently alive
##' * `logs`: a method that returns logs for a single worker
##'
##' All the methods accept a vector of worker names, or integers,
##'   except `logs` which requires a single worker id (as a string or
##'   integer). For all methods except `logs`, the default of `NULL`
##'   means "all managed workers".
rrq_worker_spawn <- function(obj, n = 1, logdir = NULL,
                             timeout = 600, worker_config = "localhost",
                             worker_id_base = NULL,
                             time_poll = 1, progress = NULL) {
  ret <- rrq_worker_manager$new(obj, n, logdir, worker_config, worker_id_base)
  if (timeout > 0) {
    ret$wait_alive(timeout, time_poll, progress)
  }
  ret
}


##' Register that workers are expected.  This generates a key that one
##' or more workers will write to when they start up (as used by
##' \code{rrq_worker_spawn}).
##'
##' The general pattern here is that the process that submits the
##' worker (so the parent process, or the user submitting a cluster
##' job) would run this function to register that some number of
##' workers will be started at some point in the future.  In the case
##' of starting workers by submitting them to a cluster, this could
##' take a long time as the job queues, or if starting by running
##' another process it could be very quick.  Information about the
##' workers that are expected and where to find them is stored against
##' a key, which is returned by `rrq_worker_expect`.
##'
##' Then, to wait on a set of workers, you run `rrq_worker_wait`
##'
##' @title Register expected workers
##'
##' @param obj A rrq_controller object
##'
##' @param worker_ids Ids of expected workers
##'
##' @return
##'
##' * For `rrq_worker_expect`: A string, which can be passed through
##'   as the second argument to `rrq_worker_wait` in order to block
##'   until workers are available.
##'
##' * For `rrq_worker_wait`: Invisibly a difftime object with the time
##'   spent waiting.
##'
##' @export
rrq_worker_expect <- function(obj, worker_ids) {
  assert_is(obj, "rrq_controller")
  con <- obj$con
  keys <- rrq_keys(obj$queue_id)

  key_alive <- rrq_key_worker_alive(obj$queue_id)
  obj$con$HMSET(keys$worker_alive, worker_ids, rep_along(key_alive, worker_ids))
  obj$con$HSET(keys$worker_expect, key_alive, object_to_bin(worker_ids))

  key_alive
}


##' @param key_alive A key as returned by `rrq_worker_expect`
##'
##' @param timeout Number of seconds to wait for workers to appear. If
##'   they have not appeared by this time, we throw an error.
##'
##' @param poll Poll interval, in seconds. Must be an integer
##'
##' @param progress Optional logical indicating if a progress bar
##'   should be displayed. If `NULL` we fall back on the value of the
##'   global option `rrq.progress`, and if that is unset display a
##'   progress bar if in an interactive session.
##'
##' @rdname rrq_worker_expect
##' @export
rrq_worker_wait <- function(obj, key_alive, timeout = Inf, poll = 1,
                            progress = NULL) {
  assert_is(obj, "rrq_controller")
  keys <- rrq_keys(obj$queue_id)
  is_dead <- function() FALSE
  path_logs <- NULL
  worker_wait_alive(obj$con, keys, key_alive, is_dead, path_logs,
                    timeout, poll, progress)
}


rrq_worker_manager <- R6::R6Class(
  "rrq_worker_manager",

  private = list(
    con = NULL,
    keys = NULL,
    process = NULL,
    logfile = NULL,
    key_alive = NULL,
    worker_id_base = NULL,

    check_worker_id = function(worker_id) {
      if (is.null(worker_id)) {
        return(self$id)
      }
      if (is.numeric(worker_id)) {
        assert_integer_like(worker_id)
        worker_id <- sprintf("%s_%d", private$worker_id_base, worker_id)
      }
      assert_character(worker_id)
      err <- !(worker_id %in% self$id)
      if (any(err)) {
        stop(sprintf("Worker not controlled by this manager: %s",
                     paste(squote(worker_id), collapse = ", ")))
      }
      worker_id
    }
  ),

  public = list(
    id = NULL,

    initialize = function(obj, n, logdir = NULL, worker_config = "localhost",
                          worker_id_base = NULL) {
      assert_is(obj, "rrq_controller")
      if (!(worker_config %in% obj$worker_config_list())) {
        stop(sprintf("Invalid rrq worker configuration key '%s'",
                     worker_config))
      }
      worker_id_base <- worker_id_base %||% ids::adjective_animal()
      worker_ids <- sprintf("%s_%d", worker_id_base, seq_len(n))
      key_alive <- rrq_worker_expect(obj, worker_ids)
      logdir <- logdir %||% tempfile()
      dir.create(logdir, FALSE, TRUE)
      con <- obj$con
      keys <- rrq_keys(obj$queue_id)

      logfile <- file.path(logdir, worker_ids)
      con$HMSET(keys$worker_process, worker_ids, logfile)

      message(sprintf("Spawning %d %s with prefix %s",
                      n, ngettext(n, "worker", "workers"), worker_id_base))

      args <- list(keys$queue_id, worker_config, key_alive, con$config())
      process <- set_names(vector("list", n), worker_ids)
      for (i in seq_len(n)) {
        args_i <- c(list(worker_ids[[i]]), args)
        ## NOTE: use cleanup = FALSE here because this stops a fight
        ## between finalizers and preserves the old behaviour of
        ## spawned workers being allowed to outlive their parents. We
        ## will rarely want the default callr/processx cleanup as we
        ## want to tidy away the worker first.
        process[[i]] <- callr::r_bg(
          function(worker_id, queue_id, worker_config, key_alive, config) {
            con <- redux::hiredis(config)
            w <- rrq_worker_from_config(
              queue_id, worker_config = worker_config, worker_id = worker_id,
              con = con)
            w$loop()
          },
          args = args_i, package = TRUE, supervise = FALSE, cleanup = FALSE,
          stdout = logfile[[i]], stderr = logfile[[i]])
      }

      private$con <- con
      private$keys <- keys
      private$process <- process
      private$logfile <- set_names(logfile, worker_ids)
      private$key_alive <- key_alive
      private$worker_id_base <- worker_id_base

      self$id <- worker_ids
      lockBinding("id", self)
    },

    logs = function(worker_id) {
      assert_scalar(worker_id)
      worker_id <- private$check_worker_id(worker_id)
      logfile <- private$logfile[[worker_id]]
      readLines(logfile)
    },

    kill = function(worker_id = NULL) {
      worker_id <- private$check_worker_id(worker_id)
      for (id in worker_id) {
        private$process[[id]]$kill()
      }
    },

    stop = function(worker_id = NULL, ...) {
      worker_id <- private$check_worker_id(worker_id)
      worker_stop(private$con, private$keys, worker_id, ...)
    },

    is_alive = function(worker_id = NULL) {
      vlapply(private$process[private$check_worker_id(worker_id)],
              function(p) p$is_alive())
    },

    wait_alive = function(timeout, poll = 1, progress = NULL) {
      con <- private$con
      is_dead <- function() {
        !vlapply(private$process, function(p) p$is_alive())
      }
      worker_wait_alive(private$con, private$keys, private$key_alive,
                        is_dead, self$logs, timeout, poll, progress)
    }
  ))


worker_wait_alive <- function(con, keys, key_alive, is_dead, path_logs,
                              timeout, poll, progress) {
  assert_scalar_numeric(timeout)
  assert_scalar_integer_like(poll)

  bin <- con$HGET(keys$worker_expect, key_alive)
  if (is.null(bin)) {
    stop("No workers expected on that key")
  }
  worker_ids <- bin_to_object(bin)

  worker_done <- function() {
    worker_ids %in% list_to_character(con$SMEMBERS(keys$worker_id)) | is_dead()
  }

  fetch <- function() {
    con$BLPOP(key_alive, poll)
    worker_done()
  }

  t0 <- Sys.time()
  done <- worker_done()
  if (!all(done)) {
    done <- general_poll(fetch, 0, timeout, "workers", FALSE, progress)
  }

  ok <- done & !is_dead()
  if (!all(ok)) {
    message(sprintf("%d / %d workers not identified in time",
                    sum(!ok), length(done)))
    if (!is.null(path_logs)) {
      logs <- set_names(lapply(worker_ids[!ok], path_logs), worker_ids[!ok])
      worker_print_failed_logs(logs)
    }
    stop("Not all workers recovered")
  }

  invisible(Sys.time() - t0)
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
