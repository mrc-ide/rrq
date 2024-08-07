% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rrq_workers.R
\name{rrq_worker_detect_exited}
\alias{rrq_worker_detect_exited}
\title{Detect exited workers}
\usage{
rrq_worker_detect_exited(controller = NULL)
}
\arguments{
\item{controller}{The controller to use.  If not given (or \code{NULL})
we'll use the controller registered with
\code{\link[=rrq_default_controller_set]{rrq_default_controller_set()}}.}
}
\value{
Undefined.
}
\description{
Detects exited workers through a lapsed heartbeat.  This differs
from \code{\link[=rrq_worker_list_exited]{rrq_worker_list_exited()}} which lists workers that have
definitely exited by checking to see if any worker that runs a
heartbeat process has not reported back in time, then marks that
worker as exited.  See vignette("fault-tolerance") for details.
}
\examples{
\dontshow{if (rrq:::enable_examples(require_queue = "rrq:example")) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
obj <- rrq_controller("rrq:example")
rrq_worker_detect_exited(controller = obj)
\dontshow{\}) # examplesIf}
}
