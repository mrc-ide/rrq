##' Post a task progress update.  The progress system in \code{rrq} is
##' agnostic about how you are going to render your progress, and so
##' it just a convention - see Details below.  Any R object can be
##' sent as a progress value (e.g., a string, a list, etc).
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
##' It is also possible to register progress \emph{without} acquiring
##' a dependency on \code{rrq}.  If your package/script includes code
##' like:
##'
##' \preformatted{
##' progress <- function(message) {
##'   signalCondition(structure(list(message = message),
##'                             class = c("progress", "condition")))
##' }
##' }
##'
##' (this function can be called anything - the important bit is the
##' body function body - you must return an object with a
##' \code{message} element and the two class attributes
##' \code{progress} and \code{condition}).
##'
##' then you can use this in the same way as
##' \code{rrq::rrq_task_progress_update} above in your code.  When run
##' without using rrq, this function will appear to do nothing.
##'
##' @title Post task update
##'
##' @param value An R object with the contents of the update. This
##'   will overwrite any previous progress value, and can be retrieved
##'   from a \code{\link{rrq_controller}} with the
##'   \code{task_progress} method.  A value of \code{NULL} will appear
##'   to clear the status, as \code{NULL} will also be returned if no
##'   status is found for a task.
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
  worker$progress(value, error)
}
