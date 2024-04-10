##' Post a task progress update.  The progress system in `rrq` is
##' agnostic about how you are going to render your progress, and so
##' it just a convention - see Details below.  Any R object can be
##' sent as a progress value (e.g., a string, a list, etc).
##'
##' In order to report on progress, a task may, in it's code, write
##'
##' ```
##' rrq::rrq_task_progress_update("task is 90% done")
##' ```
##'
##' and this information will be fetchable by calling
##' [rrq_task_progress] with the `task_id`.
##'
##' It is also possible to register progress *without* acquiring
##' a dependency on `rrq`.  If your package/script includes code
##' like:
##'
##' ```
##' progress <- function(message) {
##'   signalCondition(structure(list(message = message),
##'                             class = c("progress", "condition")))
##' }
##' ```
##'
##' (this function can be called anything - the important bit is the
##' body function body - you must return an object with a `message`
##' element and the two class attributes `progress` and `condition`).
##'
##' then you can use this in the same way as
##' `rrq::rrq_task_progress_update` above in your code.  When run
##' without using rrq, this function will appear to do nothing.
##'
##' @title Post task update
##'
##' @param value An R object with the contents of the update. This
##'   will overwrite any previous progress value, and can be retrieved
##'   by calling [rrq_task_progress].  A value of `NULL` will appear
##'   to clear the status, as `NULL` will also be returned if no
##'   status is found for a task.
##'
##' @param error Logical, indicating if we should throw an error if
##'   not running as an `rrq` task. Set this to `FALSE` if you want
##'   code to work without modification within and outside of an `rrq`
##'   job, or to `TRUE` if you want to be sure that progress messages
##'   have made it to the server.
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
