% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rrq_retry.R
\name{rrq_task_retry}
\alias{rrq_task_retry}
\title{Retry tasks}
\usage{
rrq_task_retry(task_ids, controller = NULL)
}
\arguments{
\item{task_ids}{Task ids to retry.}

\item{controller}{The controller to use.  If not given (or \code{NULL})
we'll use the controller registered with
\code{\link[=rrq_default_controller_set]{rrq_default_controller_set()}}.}
}
\value{
New task ids
}
\description{
Retry a task (or set of tasks). Typically this is after failure
(e.g., \code{ERROR}, \code{DIED} or similar) but you can retry even
successfully completed tasks. Once retried, functions that
retrieve information about a task (e.g., \code{\link[=rrq_task_status]{rrq_task_status()}}\verb{, [rrq_task_result()]) will behave differently depending on the value of their }follow\verb{argument. See}vignette("fault-tolerance")` for more details.
}
\examples{
\dontshow{if (rrq:::enable_examples(require_queue = "rrq:example")) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
obj <- rrq_controller("rrq:example")

# It's straightforward to see the effect of retrying a task with
# one that produces a different value each time, so here, we use a
# simple task that draws one normally distributed random number
t1 <- rrq_task_create_expr(rnorm(1), controller = obj)
rrq_task_wait(t1, controller = obj)
rrq_task_result(t1, controller = obj)

# If we retry the task we'll get a different value:
t2 <- rrq_task_retry(t1, controller = obj)
rrq_task_wait(t2, controller = obj)
rrq_task_result(t2, controller = obj)

# Once a task is retried, most of the time (by default) you can use
# the original id and the new one exchangeably:
rrq_task_result(t1, controller = obj)
rrq_task_result(t2, controller = obj)

# Use the 'follow' argument to modify this behaviour
rrq_task_result(t1, follow = FALSE, controller = obj)
rrq_task_result(t2, follow = FALSE, controller = obj)

# See the retry chain with rrq_task_info
rrq_task_info(t1, controller = obj)
rrq_task_info(t2, controller = obj)
\dontshow{\}) # examplesIf}
}
