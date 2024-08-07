% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rrq_workers.R
\name{rrq_worker_load}
\alias{rrq_worker_load}
\title{Report on worker load}
\usage{
rrq_worker_load(worker_ids = NULL, controller = NULL)
}
\arguments{
\item{worker_ids}{Optional vector of worker ids. If \code{NULL} then
all active workers are used.}

\item{controller}{The controller to use.  If not given (or \code{NULL})
we'll use the controller registered with
\code{\link[=rrq_default_controller_set]{rrq_default_controller_set()}}.}
}
\value{
An object of class "worker_load", which has a pretty print
method.
}
\description{
Report on worker "load" (the number of workers being used over
time). Reruns an object of class \code{worker_load}, for which a
\code{mean} method exists (this function is a work in progress and the
interface may change).
}
\examples{
\dontshow{if (rrq:::enable_examples(require_queue = "rrq:example")) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
obj <- rrq_controller("rrq:example")
mean(rrq_worker_load(controller = obj))
\dontshow{\}) # examplesIf}
}
