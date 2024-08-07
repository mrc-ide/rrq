% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rrq_task.R
\name{rrq_task_list}
\alias{rrq_task_list}
\title{List all tasks}
\usage{
rrq_task_list(controller = NULL)
}
\arguments{
\item{controller}{The controller to use.  If not given (or \code{NULL})
we'll use the controller registered with
\code{\link[=rrq_default_controller_set]{rrq_default_controller_set()}}.}
}
\value{
A character vector
}
\description{
List all tasks.  This may be a lot of tasks, and so can be quite
slow to execute.
}
\examples{
\dontshow{if (rrq:::enable_examples()) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}

obj <- rrq_controller("rrq:example")

rrq_task_list(controller = obj)
\dontshow{\}) # examplesIf}
}
