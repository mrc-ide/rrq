% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rrq_workers.R
\name{rrq_worker_exists}
\alias{rrq_worker_exists}
\title{Test if a worker exists}
\usage{
rrq_worker_exists(name, controller = NULL)
}
\arguments{
\item{name}{Name of the worker}

\item{controller}{The controller to use.  If not given (or \code{NULL})
we'll use the controller registered with
\code{\link[=rrq_default_controller_set]{rrq_default_controller_set()}}.}
}
\value{
A logical value
}
\description{
Test if a worker exists
}
