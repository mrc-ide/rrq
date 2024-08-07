% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rrq_workers.R
\name{rrq_worker_stop}
\alias{rrq_worker_stop}
\title{Stop workers}
\usage{
rrq_worker_stop(
  worker_ids = NULL,
  type = "message",
  timeout = 0,
  time_poll = 0.1,
  progress = NULL,
  controller = NULL
)
}
\arguments{
\item{worker_ids}{Optional vector of worker ids. If \code{NULL} then
all active workers will be stopped.}

\item{type}{The strategy used to stop the workers. Can be \code{message},
\code{kill} or \code{kill_local} (see Details).}

\item{timeout}{Optional timeout; if greater than zero then we poll
for a response from the worker for this many seconds until they
acknowledge the message and stop (only has an effect if \code{type}
is \code{message}). If a timeout of greater than zero is given, then
for a \code{message}-based stop we wait up to this many seconds for the
worker to exit. That means that we might wait up to \code{2 * timeout}
seconds for this function to return.}

\item{time_poll}{If \code{type} is \code{message} and \code{timeout} is greater
than zero, this is the polling interval used between redis calls.
Increasing this reduces network load but decreases the ability
to interrupt the process.}

\item{progress}{Optional logical indicating if a progress bar
should be displayed. If \code{NULL} we fall back on the value of the
global option \code{rrq.progress}, and if that is unset display a
progress bar if in an interactive session.}

\item{controller}{The controller to use.  If not given (or \code{NULL})
we'll use the controller registered with
\code{\link[=rrq_default_controller_set]{rrq_default_controller_set()}}.}
}
\value{
The names of the stopped workers, invisibly.
}
\description{
Stop workers, causing them to exit.  Workers can be stopped in a
few different ways (see Details), but after executing this
function, assume that any worker targeted will no longer be
available to work on tasks.
}
\details{
The \code{type} parameter indicates the strategy used to stop workers,
and interacts with other parameters. The strategies used by the
different values are:
\itemize{
\item \code{message}, in which case a \code{STOP} message will be sent to the
worker, which they will receive after finishing any currently
running task (if \code{RUNNING}; \code{IDLE} workers will stop immediately).
\item \code{kill}, in which case a kill signal will be sent via the heartbeat
(if the worker is using one). This will kill the worker even if
is currently working on a task, eventually leaving that task with
a status of \code{DIED}.
\item \code{kill_local}, in which case a kill signal is sent using operating
system signals, which requires that the worker is on the same
machine as the controller.
}
}
\examples{
\dontshow{if (rrq:::enable_examples(require_queue = "rrq:example")) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
obj <- rrq_controller("rrq:example")
w <- rrq_worker_spawn(controller = obj)
rrq_worker_list(controller = obj)
rrq_worker_stop(w$id, timeout = 10, controller = obj)
rrq_worker_list(controller = obj)
\dontshow{\}) # examplesIf}
}
