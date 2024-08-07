% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rrq_create.R
\name{rrq_task_create_bulk_call}
\alias{rrq_task_create_bulk_call}
\title{Create bulk tasks from a call}
\usage{
rrq_task_create_bulk_call(
  fn,
  data,
  args = NULL,
  queue = NULL,
  separate_process = FALSE,
  timeout_task_run = NULL,
  depends_on = NULL,
  controller = NULL
)
}
\arguments{
\item{fn}{The function to call}

\item{data}{The data to apply the function over.  This can be a
vector or list, in which case we act like \code{lapply} and apply
\code{fn} to each element in turn.  Alternatively, this can be a
\link{data.frame}, in which case each row is taken as a set of
arguments to \code{fn}.  Note that if \code{data} is a \code{data.frame} then
all arguments to \code{fn} are named.}

\item{args}{Additional arguments to \code{fn}, shared across all calls.
These must be named.  If you are using a \code{data.frame} for
\code{data}, you'd probably be better off adding additional columns
that don't vary across rows, but the end result is the same.}

\item{queue}{The queue to add the task to; if not specified the
"default" queue (which all workers listen to) will be used. If
you have configured workers to listen to more than one queue you
can specify that here. Be warned that if you push jobs onto a
queue with no worker, it will queue forever.}

\item{separate_process}{Logical, indicating if the task should be
run in a separate process on the worker. If \code{TRUE}, then the
worker runs the task in a separate process using the \code{callr}
package. This means that the worker environment is completely
clean, subsequent runs are not affected by preceding ones.  The
downside of this approach is a considerable overhead in starting
the external process and transferring data back.}

\item{timeout_task_run}{Optionally, a maximum allowed running
time, in seconds. This parameter only has an effect if
\code{separate_process} is \code{TRUE}. If given, then if the task takes
longer than this time it will be stopped and the task status set
to \code{TIMEOUT}.}

\item{depends_on}{Vector or list of IDs of tasks which must have
completed before this job can be run. Once all dependent tasks
have been successfully run, this task will get added to the
queue. If the dependent task fails then this task will be
removed from the queue.}

\item{controller}{The controller to use.  If not given (or \code{NULL})
we'll use the controller registered with
\code{\link[=rrq_default_controller_set]{rrq_default_controller_set()}}.}
}
\value{
A vector of task identifiers; this will have the length as
\code{data} has rows if it is a \code{data.frame}, otherwise it has the
same length as \code{data}
}
\description{
Create a bulk set of tasks based on applying a function over a
vector or \link{data.frame}.  This is the bulk equivalent of
\link{rrq_task_create_call}, in the same way that
\link{rrq_task_create_bulk_expr} is a bulk version of
\link{rrq_task_create_expr}.
}
\examples{
\dontshow{if (rrq:::enable_examples(require_queue = "rrq:example")) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
obj <- rrq_controller("rrq:example")

d <- data.frame(n = 1:10, lambda = rgamma(10, 5))
ts <- rrq_task_create_bulk_call(rpois, d, controller = obj)
rrq_task_wait(ts, controller = obj)
rrq_task_results(ts, controller = obj)
\dontshow{\}) # examplesIf}
}
