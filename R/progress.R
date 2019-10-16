##' Post a task progress update.  The progress system in \code{rrq} is
##' agnostic about how you are going to render your progress, and so
##' it just a convention - see Details below.
##'
##' In order to report on progress, a task may, in it's code, write
##'
##' \preformatted{
##' rrq::rrq_task_progress_update("task is 90% done")
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
##' @param error Logical, indicating if we should throw an error if
##'   not running as an \code{rrq} task. Set this to \code{FALSE} if
##'   you want code to work without modification within and outside of
##'   an `rrq` job, or to \code{TRUE} if you want to be sure that
##'   progress messages have made it to the server.
##'
##' @export
rrq_task_progress_update <- function(value, error = FALSE) {
  worker <- cache$active_worker
  if (is.null(worker)) {
    if (error) {
      stop("rrq_task_progress_update called with no active worker")
    } else {
      return(invisible())
    }
  }
  task_progress_update(value, worker, error)
}


task_progress_update <- function(value, worker, error) {
  assert_is(worker, "rrq_worker")
  task_id <- worker$active_task$task_id
  if (is.null(task_id)) {
    if (error) {
      stop("rrq_task_progress_update called with no active task")
    } else {
      return(invisible())
    }
  }
  assert_scalar_character(value)
  worker$con$HSET(worker$keys$task_progress, task_id, value)
  invisible()
}
