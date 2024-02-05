##' Create a new controller.  This is the new interface that will
##' replace [rrq_controller] soon, at which point it will rename back
##' to `rrq_controller`.
##'
##' @title Create rrq controller
##'
##' @param queue_id An identifier for the queue.  This will prefix all
##'   keys in redis, so a prefix might be useful here depending on
##'   your use case (e.g. \code{rrq:<user>:<id>})
##'
##' @param con A redis connection. The default tries to create a redis
##'   connection using default ports, or environment variables set as in
##'   [redux::hiredis()]
##'
##' @param timeout_task_wait An optional default timeout to use when
##'   waiting for tasks (e.g., with `rrq_task_wait`, `rrq_tasks_wait`,
##'   etc). If not given, then we fall back on the global option
##'   `rrq.timeout_task_wait`, and if that is not set, we wait forever
##'   (i.e., `timeout_task_wait = Inf`).
##'
##' @param follow An optional default logical to use for tasks
##'   that may (or may not) be retried. If not given we fall back
##'   on the global option `rrq.follow`, and if that is not set then
##'   `TRUE` (i.e., we do follow). The value `follow = TRUE` is
##'   potentially slower than `follow = FALSE` for some operations
##'   because we need to dereference every task id. If you never use
##'   `rrq_task_retry` then this dereference never has an effect and we
##'   can skip it. See `vignette("fault-tolerance")` for more
##'   information.
##'
##' @return An `rrq_controller2` object, which is opaque.
##' @export
rrq_controller2 <- function(queue_id, con = redux::hiredis,
                            timeout_task_wait = NULL, follow = NULL) {
  ## We'll move the construction code here shortly, but this way makes
  ## the migration a little easier.
  rrq_controller$new(queue_id, con, timeout_task_wait, follow)$to_v2()
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
