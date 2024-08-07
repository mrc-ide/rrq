% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rrq_task.R
\name{rrq_task_wait}
\alias{rrq_task_wait}
\title{Wait for group of tasks}
\usage{
rrq_task_wait(
  task_id,
  timeout = NULL,
  time_poll = 1,
  progress = NULL,
  follow = NULL,
  controller = NULL
)
}
\arguments{
\item{task_id}{A vector of task ids to poll for (can be one task
or many)}

\item{timeout}{Optional timeout, in seconds, after which an error
will be thrown if the task has not completed. If not given,
falls back on the controller's \code{timeout_task_wait} (see
\link{rrq_controller})}

\item{time_poll}{Optional time with which to "poll" for
completion.  By default this will be 1 second; this is the time
that each request for a completed task may block for (however,
if the task is finished before this, the actual time waited for
will be less).  Increasing this will reduce the responsiveness
of your R session to interrupting, but will cause slightly less
network load.  Values less than 1s are only supported with Redis
server version 6.0.0 or greater (released September 2020).}

\item{progress}{Optional logical indicating if a progress bar
should be displayed. If \code{NULL} we fall back on the value of the
global option \code{rrq.progress}, and if that is unset display a
progress bar if in an interactive session.}

\item{follow}{Optional logical, indicating if we should follow any
redirects set up by doing \link{rrq_task_retry}. If not given, falls
back on the value passed into the controller, the global option
\code{rrq.follow}, and finally \code{TRUE}. Set to \code{FALSE} if you want to
return information about the original task, even if it has been
subsequently retried.}

\item{controller}{The controller to use.  If not given (or \code{NULL})
we'll use the controller registered with
\code{\link[=rrq_default_controller_set]{rrq_default_controller_set()}}.}
}
\value{
A scalar logical value; \code{TRUE} if \emph{all} tasks complete
successfully and \code{FALSE} otherwise
}
\description{
Wait for a task, or set of tasks, to complete.  If you have used
\code{rrq} prior to version 0.8.0, you might expect this function to
return the result, but we now return a logical value which
indicates success or not.  You can fetch the task result with
\link{rrq_task_result}.
}
\examples{
\dontshow{if (rrq:::enable_examples(require_queue = "rrq:example")) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
obj <- rrq_controller("rrq:example")
t1 <- rrq_task_create_expr(Sys.sleep(1), controller = obj)
rrq_task_wait(t1, controller = obj)

# The return value of wait gives a summary of successfullness
# of the task
t2 <- rrq_task_create_expr(stop("Some error"), controller = obj)
rrq_task_wait(t2, controller = obj)

# If you wait on many tasks, the return value is effectively
# reduced with "all" (so the result is TRUE if all tasks were
# successful)
rrq_task_wait(c(t1, t2), controller = obj)
\dontshow{\}) # examplesIf}
}
