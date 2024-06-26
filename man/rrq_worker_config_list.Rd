% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rrq_workers.R
\name{rrq_worker_config_list}
\alias{rrq_worker_config_list}
\title{List worker configurations}
\usage{
rrq_worker_config_list(controller = NULL)
}
\arguments{
\item{controller}{The controller to use.  If not given (or \code{NULL})
we'll use the controller registered with
\code{\link[=rrq_default_controller_set]{rrq_default_controller_set()}}.}
}
\value{
A character vector of names; these can be passed as the
\code{name} argument to \code{\link[=rrq_worker_config_read]{rrq_worker_config_read()}}.
}
\description{
Return names of worker configurations saved by
\code{\link[=rrq_worker_config_save]{rrq_worker_config_save()}}
}
