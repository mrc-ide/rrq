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
##' @param name_config Name of the configuration to use.  By default
##'   the \code{"localhost"} configuration is used
##'
##' @param worker_id_base Optional base to construct the worker ids
##'   from.  If omitted a random base will be used. Actual ids will be
##'   created but appending integers to this base.
##'
##' @param time_poll Polling period (in seconds) while waiting for
##'   workers to come up.
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
                             timeout = 600, name_config = "localhost",
                             worker_id_base = NULL,
                             time_poll = 0.2, progress = NULL) {
  controller <- obj$to_v2()
  rrq_worker_spawn2(n = n, logdir = logdir, timeout = timeout,
                    name_config = name_config, worker_id_base = worker_id_base,
                    time_poll = time_poll, progress = progress,
                    controller = controller)
}


##' @export
##' @rdname rrq_worker_spawn
##' @param controller The controller to use.  If not given (or `NULL`)
##'   we'll use the controller registered with
##'   [rrq_default_controller_set()].
rrq_worker_spawn2 <- function(n = 1, logdir = NULL, timeout = 600,
                              name_config = "localhost", worker_id_base = NULL,
                              time_poll = 0.2, progress = NULL,
                              controller = NULL) {
  controller <- get_controller(controller)
  manager <- rrq_worker_manager$new(controller, n, logdir, name_config,
                                    worker_id_base)
  if (timeout > 0) {
    manager$wait_alive(timeout, time_poll, progress)
  }
  manager
}


##' Wait for workers to appear.
##'
##' @title Wait for workers
##'
##' @param worker_ids A vector of worker ids to wait for
##'
##' @param timeout Timeout in seconds; default is to wait forever
##'
##' @param time_poll Poll interval, in seconds. Must be an integer
##'
##' @param progress Optional logical indicating if a progress bar
##'   should be displayed. If `NULL` we fall back on the value of the
##'   global option `rrq.progress`, and if that is unset display a
##'   progress bar if in an interactive session.
##'
##' @param controller The controller to use.  If not given (or `NULL`)
##'   we'll use the controller registered with
##'   [rrq_default_controller_set()].
##'
##' @export
rrq_worker_wait <- function(worker_ids, timeout = Inf, time_poll = 1,
                            progress = NULL, controller = NULL) {
  assert_scalar_numeric(timeout)
  assert_scalar_numeric(time_poll)
  controller <- get_controller(controller)
  is_dead <- NULL
  path_logs <- NULL
  worker_wait_alive(controller, worker_ids, is_dead, path_logs,
                    timeout, time_poll, progress,
                    call = rlang::current_env())
}


rrq_worker_manager <- R6::R6Class(
  "rrq_worker_manager",

  private = list(
    controller = NULL,
    process = NULL,
    logfile = NULL,
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
      err <- setdiff(worker_id, self$id)
      if (length(err) > 0) {
        cli::cli_abort(
          "Worker{?s} not controlled by this manager: {squote(err)}")
      }
      worker_id
    }
  ),

  public = list(
    id = NULL,

    initialize = function(controller, n, logdir = NULL,
                          name_config = "localhost",
                          worker_id_base = NULL) {
      assert_is(controller, "rrq_controller2")

      if (!(name_config %in% rrq_worker_config_list(controller))) {
        cli::cli_abort("Invalid rrq worker configuration key '{name_config}'")
      }
      worker_id_base <- worker_id_base %||% ids::adjective_animal()
      worker_ids <- sprintf("%s_%d", worker_id_base, seq_len(n))
      logdir <- logdir %||% tempfile()
      dir.create(logdir, FALSE, TRUE)

      con <- controller$con
      keys <- controller$keys

      logfile <- file.path(logdir, worker_ids)
      con$HMSET(keys$worker_process, worker_ids, logfile)

      cli::cli_alert_info(
        "Spawning {n} worker{?s} with prefix '{worker_id_base}'")

      args <- list(keys$queue_id, name_config, con$config())
      process <- set_names(vector("list", n), worker_ids)
      for (i in seq_len(n)) {
        args_i <- c(list(worker_ids[[i]]), args)
        ## NOTE: use cleanup = FALSE here because this stops a fight
        ## between finalizers and preserves the old behaviour of
        ## spawned workers being allowed to outlive their parents. We
        ## will rarely want the default callr/processx cleanup as we
        ## want to tidy away the worker first.
        process[[i]] <- callr::r_bg(
          function(worker_id, queue_id, name_config, config) {
            con <- redux::hiredis(config)
            w <- rrq_worker$new(
              queue_id, name_config = name_config, worker_id = worker_id,
              con = con)
            w$loop()
          },
          args = args_i, package = TRUE, supervise = FALSE, cleanup = FALSE,
          stdout = logfile[[i]], stderr = logfile[[i]])
      }

      private$controller <- controller
      private$process <- process
      private$logfile <- set_names(logfile, worker_ids)
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
      rrq_worker_stop(worker_ids = worker_id, ...,
                      controller = private$controller)
    },

    is_alive = function(worker_id = NULL) {
      vlapply(private$process[private$check_worker_id(worker_id)],
              function(p) p$is_alive())
    },

    wait_alive = function(timeout, time_poll = 0.2, progress = NULL) {
      assert_scalar_numeric(timeout)
      assert_scalar_numeric(time_poll)
      is_dead <- function() {
        !vlapply(private$process, function(p) p$is_alive())
      }
      worker_wait_alive(private$controller, self$id, is_dead, self$logs,
                        timeout, time_poll, progress,
                        call = rlang::current_env())
    }
  ))


worker_wait_alive <- function(controller, worker_ids, is_dead, path_logs,
                              timeout, time_poll, progress, call = NULL) {
  con <- controller$con
  keys <- controller$keys

  worker_status <- function() {
    known <- list_to_character(con$SMEMBERS(keys$worker_id))
    ret <- rep("waiting", length(worker_ids))
    ret[worker_ids  %in% known] <- "running"
    if (!is.null(is_dead)) {
      ret[is_dead()] <- "died"
    }
    ret
  }

  res <- logwatch::logwatch(
    ngettext(length(worker_ids), "worker", "workers"),
    worker_status,
    NULL,
    show_log = FALSE,
    poll = time_poll,
    timeout = timeout,
    status_waiting = "waiting",
    status_running = character(),
    multiple = length(worker_ids) > 1)

  err <- res$status != "running"
  if (any(err)) {
    if (is.null(path_logs)) {
      logs <- NULL
    } else {
      logs <- set_names(lapply(worker_ids[err], path_logs), worker_ids[err])
    }
    abort_workers_not_ready(res$status, logs, call = call)
  }

  invisible(res$end - res$start)
}


abort_workers_not_ready <- function(status, logs, call = NULL) {
  n <- length(status)
  is_dead <- status == "died"
  is_waiting <- status == "waiting"
  if (all(is_dead)) {
    if (n == 1) {
      msg <- "Worker died"
    } else {
      msg <- "All {n} workers died"
    }
  } else if (any(is_dead)) {
    msg <- "{sum(is_dead)} / {n} workers died"
  } else {
    if (n == 1) {
      msg <- "Worker not ready in time"
    } else if (all(is_waiting)) {
      msg <- "All {n} workers not ready in time"
    } else {
      msg <- "{sum(is_waiting)} / {n} workers not ready in time"
    }
  }

  cli::cli_abort(msg, footer = worker_format_failed_logs, logs = logs,
                 call = call)
}


worker_format_failed_logs <- function(err) {
  logs <- err$logs
  if (is.null(logs)) {
    c("!" = "Logging not enabled for these workers")
  } else {
    n <- length(logs)
    details <- unlist(unname(Map(c, ">" = names(logs), unname(logs), "")))
    c(i = cli::format_inline("Log files recovered for {n} worker{?s}"),
      details[-length(details)])
  }
}
