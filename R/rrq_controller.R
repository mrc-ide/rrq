##' @title rrq queue controller
##'
##' @description
##' A queue controller.  Use this to interact with a queue/cluster.
##'
##' @section Task lifecycle:
##'
##' * A task is queued with `$enqueue()`, at which point it becomes `PENDING`
##' * Once a worker selects the task to run, it becomes `RUNNING`
##' * If the task completes successfully without error it becomes `COMPLETE`
##' * If the task throws an error, it becomes `ERROR`
##' * If the task was cancelled (e.g., via `$task_cancel()`) it becomes
##'   `CANCELLED`
##' * If the task is killed by an external process, crashes or the worker
##'   dies (and is running a heartbeat) then the task becomes `DIED`.
##' * The status of an unknown task is `MISSING`
##' * Tasks in any terminal state (except `IMPOSSIBLE`) may be retried
##'   with `task_retry` at which point they become `MOVED`, see
##'   `vignette("fault-tolerance")` for details
##'
##' @section Worker lifecycle:
##'
##' * A worker appears and is `IDLE`
##' * When running a task it is `BUSY`
##' * If it receives a `PAUSE` message it becomes `PAUSED` until it
##'   receives a `RESUME` message
##' * If it exits cleanly (e.g., via a `STOP` message or a timeout) it
##'   becomes `EXITED`
##' * If it crashes and was running a heartbeat, it becomes `LOST`
##'
##' @section Messages:
##'
##' Most of the time workers process tasks, but you can also send them
##'   "messages". Messages take priority over tasks, so if a worker
##'   becomes idle (by coming online or by finishing a task) it will
##'   consume all available messages before starting on a new task,
##'   even if both are available.
##'
##' Each message has a "command" and may have "arguments" to that
##'   command. The supported messages are:
##'
##' * `PING` (no args): "ping" the worker, if alive it will respond
##'   with "PONG"
##'
##' * `ECHO` (accepts an argument of a string): Print a string to the
##'   terminal and log of the worker. Will respond with `OK` once the
##'   message has been printed.
##'
##' * `EVAL` (accepts a string or a quoted expression): Evaluate an
##'   arbitrary R expression on the worker. Responds with the value of
##'   this expression.
##'
##' * `STOP` (accepts a string to print as the worker exits, defaults
##'   to "BYE"): Tells the worker to stop.
##'
##' * `INFO` (no args): Returns information about the worker (versions
##'   of packages, hostname, pid, etc).
##'
##' * `PAUSE` (no args): Tells the worker to stop accepting tasks
##'   (until it receives a `RESUME` message). Messages are processed
##'   as normal.
##'
##' * `RESUME` (no args): Tells a paused worker to resume accepting
##'   tasks.
##'
##' * `REFRESH` (no args): Tells the worker to rebuild their
##'   environment with the `create` method.
##'
##' * `TIMEOUT_SET` (accepts a number, representing seconds): Updates
##'   the worker timeout - the length of time after which it will exit
##'   if it has not processed a task.
##'
##' * `TIMEOUT_GET` (no args): Tells the worker to respond with its
##'   current timeout.
##'
##' @param follow Optional logical, indicating if we should follow any
##'   redirects set up by doing `$task_retry`. If not given, falls
##'   back on the value passed into the queue, the global option
##'   `rrq.follow`, and finally `TRUE`. Set to `FALSE` if you want to
##'   return information about the original task, even if it has been
##'   subsequently retried.
##'
##' @export
rrq_controller <- R6::R6Class(
  "rrq_controller",
  cloneable = FALSE,

  public = list(
    ##' @field con The redis connection. This is part of the
    ##' public API and can be used to access the same redis database
    ##' as the queue.
    con = NULL,

    ##' @field queue_id The queue id used on creation. This is read-only
    ##' after creation.
    queue_id = NULL,

    ##' @description Constructor
    ##' @param queue_id An identifier for the queue.  This will prefix all
    ##'   keys in redis, so a prefix might be useful here depending on
    ##'   your use case (e.g. \code{rrq:<user>:<id>})
    ##'
    ##' @param con A redis connection. The default tries to create a redis
    ##'   connection using default ports, or environment variables set as in
    ##'   [redux::hiredis()]
    ##'
    ##' @param timeout_task_wait An optional default timeout to use when
    ##'   waiting for tasks (e.g., with `$task_wait()`, `$tasks_wait()`,
    ##'   etc). If not given, then we fall back on the
    ##'   global option `rrq.timeout_task_wait`, and if that is not set,
    ##    we wait forever (i.e., `timeout_task_wait = Inf`).
    ##'
    ##' @param follow An optional default logical to use for tasks
    ##'   that may (or may not) be retried. If not given we fall back
    ##'   on the global option `rrq.follow`, and if that is not set then
    ##'   `TRUE` (i.e., we do follow). The value `follow = TRUE` is
    ##'   potentially slower than `follow = FALSE` for some operations
    ##'   because we need to dereference every task id. If you never use
    ##'   `$task_retry` then this dereference never has an effect and we
    ##'   can skip it. See `vignette("fault-tolerance")` for more
    ##'   information.
    ##'
    ##' @param check_version Check that the schema version is correct
    initialize = function(queue_id, con = redux::hiredis(),
                          timeout_task_wait = NULL, follow = NULL,
                          check_version = TRUE) {
      assert_scalar_character(queue_id)
      assert_is(con, "redis_api")

      self$con <- con
      self$queue_id <- queue_id
      private$keys <- rrq_keys(self$queue_id)

      if (is.null(timeout_task_wait)) {
        private$timeout_task_wait <- getOption("rrq.timeout_task_wait", Inf)
      } else {
        assert_scalar_positive_integer(timeout_task_wait)
        private$timeout_task_wait <- timeout_task_wait
      }

      if (is.null(follow)) {
        private$follow <- getOption("rrq.follow", TRUE)
      } else {
        assert_scalar_logical(follow)
        private$follow <- follow
      }

      if (check_version) {
        rrq_version_check(self$con, private$keys)
      }
      self$worker_config_save(WORKER_CONFIG_DEFAULT, rrq_worker_config(),
                              overwrite = FALSE)

      private$store <- rrq_object_store(self$con, private$keys)
      private$scripts <- rrq_scripts_load(self$con)
      info <- object_to_bin(controller_info())
      rpush_max_length(self$con, private$keys$controller, info, 10)

      lockBinding("queue_id", self)
    },

    ##' @description Convert controller to the new-style object.
    ##'   Please don't use this in packages directly
    to_v2 = function() {
      ret <- list(queue_id = private$keys$queue_id,
                  con = self$con,
                  keys = private$keys,
                  timeout_task_wait = private$timeout_task_wait,
                  follow = private$follow,
                  scripts = private$scripts,
                  store = private$store)
      class(ret) <- "rrq_controller2"
      ret
    },

    ##' @description Save a worker configuration, which can be used to
    ##' start workers with a set of options with the cli. These
    ##' correspond to arguments to [rrq::rrq_worker].
    ##'
    ##' @param name Name for this configuration
    ##'
    ##' @param config A worker configuration, created by
    ##'   [rrq::rrq_worker_config()]
    ##'
    ##' @param overwrite Logical, indicating if an existing configuration
    ##'   with this `name` should be overwritten if it exists. If `FALSE`,
    ##'   then the configuration is not updated, even if it differs from
    ##'   the version currently saved.
    ##'
    ##' @return Invisibly, a boolean indicating if the configuration was
    ##'   updated.
    worker_config_save = function(name, config, overwrite = TRUE) {
      rrq_worker_config_save2(name, config, overwrite, self)
    }
  ),

  private = list(
    keys = NULL,
    timeout_task_wait = NULL,
    follow = NULL,
    scripts = NULL,
    store = NULL
  ))



task_submit_n <- function(controller, task_ids, dat, key_complete, queue,
                          separate_process, timeout, depends_on = NULL) {
  con <- controller$con
  keys <- controller$keys

  n <- length(dat)
  queue <- queue %||% QUEUE_DEFAULT
  key_queue <- rrq_key_queue(keys$queue_id, queue)

  local <- if (separate_process) "FALSE" else "TRUE"

  if (!is.null(timeout)) {
    if (!separate_process) {
      stop("Can't set timeout as 'separate_process' is FALSE")
    }
    timeout <- list(
      redis$HMSET(keys$task_timeout, task_ids, as.character(timeout)))
  }

  depends_up_original_keys <- rrq_key_task_depends_up_original(
    keys$queue_id, task_ids)
  depends_up_keys <- rrq_key_task_depends_up(keys$queue_id, task_ids)
  depends_down_keys <- rrq_key_task_depends_down(keys$queue_id, depends_on)

  cmds <- list()

  time <- timestamp()
  cmds <- c(
    cmds,
    list(
      redis$HMSET(keys$task_expr, task_ids, dat),
      redis$HMSET(keys$task_status, task_ids, rep_len(TASK_PENDING, n)),
      redis$HMSET(keys$task_queue, task_ids, rep_len(queue, n)),
      redis$HMSET(keys$task_local, task_ids, rep_len(local, n)),
      redis$HMSET(keys$task_time_submit, task_ids, rep_len(time, n))),
    timeout)
  if (length(depends_on) > 0) {
    cmds <- c(
      cmds,
      list(
        status = redis$HMGET(keys$task_status, depends_on),
        redis$HMSET(keys$task_status, task_ids, rep_len(TASK_DEFERRED, n))),
      lapply(depends_up_original_keys, redis$SADD, depends_on),
      lapply(depends_up_keys, redis$SADD, depends_on),
      lapply(depends_down_keys, redis$SADD, task_ids))
  } else {
    cmds <- c(cmds, list(redis$RPUSH(key_queue, task_ids)))
  }
  response <- con$pipeline(.commands = cmds)

  ## If any dependencies will never be satisfied then cleanup and error
  ## We do it this way around i.e. queue then check status of dependencies to
  ## avoid a race condition. If we were to check status of dependencies
  ## then queue we could get into condition where e.g.
  ## 1. Run report B which depends on report A
  ## 2. Check status of A and it is running
  ## 3. Add B to the queue
  ## In the time between 2 and 3 A could have finished and failed meaning that
  ## the dependency of B will never be satisfied and it will never be run.
  if (any(response$status %in% TASK$terminal_fail)) {
    run_task_cleanup_failure(controller, task_ids, TASK_IMPOSSIBLE, NULL)
    incomplete <- response$status[response$status %in% TASK$terminal_fail]
    names(incomplete) <- depends_on[response$status %in% TASK$terminal_fail]
    stop(sprintf("Failed to queue as dependent tasks failed:\n%s",
                 paste0(paste0(names(incomplete), ": ", incomplete),
                        collapse = ", ")))
  }

  complete <- depends_on[response$status == TASK_COMPLETE]
  for (dep_id in complete) {
    queue_dependencies(controller, dep_id, task_ids)
  }

  task_ids
}


controller_info <- function() {
  list(hostname = hostname(),
       pid = process_id(),
       username = username(),
       time = Sys.time())
}

worker_naturalsort <- function(x) {
  re <- "^(.*)_(\\d+)$"
  root <- sub(re, "\\1", x)
  i <- grepl(re, x)
  idx <- numeric(length(x))
  idx[i] <- as.integer(sub(re, "\\2", x[i]))
  x[order(root, idx)]
}


rrq_object_store <- function(con, keys) {
  config <- rrq_configure_read(con, keys)
  if (is.null(config$offload_path)) {
    offload <- object_store_offload_null$new()
  } else {
    offload <- object_store_offload_disk$new(config$offload_path)
  }
  object_store$new(con, keys$object_store,
                   config$store_max_size, offload)
}


verify_dependencies_exist <- function(controller, depends_on) {
  if (!is.null(depends_on)) {
    dependencies_exist <- rrq_task_exists(depends_on, named = TRUE,
                                          controller = controller)
    if (!all(dependencies_exist)) {
      missing <- names(dependencies_exist[!dependencies_exist])
      error_msg <- ngettext(
        length(missing),
        "Failed to queue as dependency %s does not exist.",
        "Failed to queue as dependencies %s do not exist.")
      stop(sprintf(error_msg, paste0(missing, collapse = ", ")))
    }
  }
  invisible(TRUE)
}
