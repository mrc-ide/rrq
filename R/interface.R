##' Create a new controller.  This is the new interface that will
##' replace [rrq_controller] soon, at which point it will rename back
##' to `rrq_controller`.
##'
##' # Task lifecycle
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
##' # Worker lifecycle
##'
##' * A worker appears and is `IDLE`
##' * When running a task it is `BUSY`
##' * If it receives a `PAUSE` message it becomes `PAUSED` until it
##'   receives a `RESUME` message
##' * If it exits cleanly (e.g., via a `STOP` message or a timeout) it
##'   becomes `EXITED`
##' * If it crashes and was running a heartbeat, it becomes `LOST`
##'
##' # Messages
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
##' @title Create rrq controller
##'
##' @param queue_id An identifier for the queue.  This will prefix all
##'   keys in redis, so a prefix might be useful here depending on
##'   your use case (e.g. `rrq:<user>:<id>`)
##'
##' @param con A redis connection. The default tries to create a redis
##'   connection using default ports, or environment variables set as in
##'   [redux::hiredis()]
##'
##' @param timeout_task_wait An optional default timeout to use when
##'   waiting for tasks with [rrq_task_wait]. If not given, then we
##'   fall back on the global option `rrq.timeout_task_wait`, and if
##'   that is not set, we wait forever (i.e., `timeout_task_wait =
##'   Inf`).
##'
##' @param follow An optional default logical to use for tasks
##'   that may (or may not) be retried. If not given we fall back
##'   on the global option `rrq.follow`, and if that is not set then
##'   `TRUE` (i.e., we do follow). The value `follow = TRUE` is
##'   potentially slower than `follow = FALSE` for some operations
##'   because we need to dereference every task id. If you never use
##'   [rrq_task_retry] then this dereference never has an effect and we
##'   can skip it. See `vignette("fault-tolerance")` for more
##'   information.
##'
##' @param check_version Logical, indicating if we should check the
##'   schema version.  You can pass `FALSE` here to continue even
##'   where the schema version is incompatible, though any subsequent
##'   actions may lead to corruption.
##'
##' @return An `rrq_controller2` object, which is opaque.
##' @export
rrq_controller2 <- function(queue_id, con = redux::hiredis(),
                            timeout_task_wait = NULL, follow = NULL,
                            check_version = TRUE) {
  assert_scalar_character(queue_id)
  assert_is(con, "redis_api")

  keys <- rrq_keys(queue_id)

  if (is.null(timeout_task_wait)) {
    timeout_task_wait <- getOption("rrq.timeout_task_wait", Inf)
  } else {
    assert_scalar_positive_integer(timeout_task_wait)
  }

  if (is.null(follow)) {
    follow <- getOption("rrq.follow", TRUE)
  } else {
    assert_scalar_logical(follow)
  }

  if (check_version) {
    rrq_version_check(con, keys)
  }
  
  ret <- list(queue_id = keys$queue_id,
              con = con,
              keys = keys,
              timeout_task_wait = timeout_task_wait,
              follow = follow,
              scripts = rrq_scripts_load(con),
              store = rrq_object_store(con, keys))
  class(ret) <- "rrq_controller2"

  rrq_worker_config_save2(WORKER_CONFIG_DEFAULT, rrq_worker_config(),
                          overwrite = FALSE, controller = ret)

  info <- object_to_bin(controller_info())
  rpush_max_length(con, keys$controller, info, 10)

  ret
}


##' Set or clear a default controller for use with rrq functions.  You
##' will want to use this to avoid passing `controller` in as a named
##' argument to every function.
##'
##' @title Register default controller
##'
##' @param controller An rrq_controller2 object
##'
##' @export
rrq_default_controller_set <- function(controller) {
  assert_is(controller, "rrq_controller2")
  pkg$default_controller <- controller
  invisible(controller)
}

##' @rdname rrq_default_controller_set
##' @export
rrq_default_controller_clear <- function() {
  pkg$default_controller <- NULL
}


pkg <- new.env(parent = emptyenv())


get_controller <- function(controller, call = NULL) {
  if (!is.null(controller)) {
    if (inherits(controller, "rrq_controller")) {
      return(controller$to_v2())
    }
    assert_is(controller, "rrq_controller2")
    return(controller)
  }
  res <- pkg$default_controller
  if (is.null(res)) {
    cli::cli_abort(c(
      "Default controller not set",
      i = "Use 'rrq_default_controller_set()', or pass one explicitly"),
      call = call, arg = "controller")
  }
  res
}


##' @export
print.rrq_controller2 <- function(x, ...) {
  cat(sprintf("<rrq_controller: %s>\n", x$queue_id))
  invisible(x)
}
