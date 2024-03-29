% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rrq_task.R
\name{rrq_task_cancel}
\alias{rrq_task_cancel}
\title{Cancel a task}
\usage{
rrq_task_cancel(task_id, wait = TRUE, timeout_wait = 10, controller = NULL)
}
\arguments{
\item{task_id}{Id of the task to cancel}

\item{wait}{Wait for the task to be stopped, if it was running.}

\item{timeout_wait}{Maximum time, in seconds, to wait for the task
to be cancelled by the worker.}

\item{controller}{The controller to use.  If not given (or \code{NULL})
we'll use the controller registered with
\code{\link[=rrq_default_controller_set]{rrq_default_controller_set()}}.}
}
\value{
Nothing if successfully cancelled, otherwise throws an
error with task_id and status e.g. Task 123 is not running (MISSING)
}
\description{
Cancel a single task. If the task is \code{PENDING} it will be unqueued
and the status set to \code{CANCELED}.  If \code{RUNNING} then the task will
be stopped if it was set to run in a separate process (i.e.,
queued with \code{separate_process = TRUE}).  Dependent tasks will be
marked as impossible.
}
