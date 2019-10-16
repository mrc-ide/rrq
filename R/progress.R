##' Post a task progress update.  The progress system in \code{rrq} is
##' agnostic about how you are going to render your progress, and so
##' it just a convention - see Details below.
##'
##' In order to report on progress, a task may, in it's code, write
##'
##' \preformatted{
##' rrq::rrq_progress_update("task is 90% done")
##' }
##'
##' and this information will be fetchable from \code{rrq_controller}
##' using the \code{task_progress} method and providing the
##' \code{task_id}.
##'
##' The progress value must be a single string.  We may extend this in
##' a future version to allow serialising an arbitrary R object,
##' though this adds complication in reading the status later.
##'
##' @title Post task update
##'
##' @param value A string with the contents of the update. This will
##'   overwrite any previous progress value, and can be retrieved from
##'   a \code{\link{rrq_controller}} with the \code{task_progress}
##'   method.
##'
##' @export
rrq_progress_update <- function(value) {
  worker <- cache$active_worker
  if (is.null(worker)) {
    stop("rrq_progress_update can be called only when a worker is active")
  }
  progress_update(value, worker)
}


progress_update <- function(value, worker) {
  assert_is(worker, "rrq_worker")
  task_id <- worker$active_task$task_id
  if (is.null(task_id)) {
    stop("Can't register rrq progress as no task running")
  }
  assert_scalar_character(value)
  worker$con$HSET(worker$keys$task_progress, task_id, value)
  invisible()
}
