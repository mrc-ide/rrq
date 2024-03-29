% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/heartbeat_impl.R
\name{rrq_heartbeat_kill}
\alias{rrq_heartbeat_kill}
\title{Kill a process running a heartbeat}
\usage{
rrq_heartbeat_kill(con, key, signal = tools::SIGTERM)
}
\arguments{
\item{con}{A hiredis object}

\item{key}{The heartbeat key}

\item{signal}{A signal to send (typically \code{tools::SIGTERM} for a
"polite" shutdown)}
}
\description{
Send a kill signal (typically \code{SIGTERM}) to terminate a process
that is running a heartbeat. This is used by
\code{\link{rrq_controller}} in order to tear down workers, even if
they are processing a task. When a heartbeat process is created,
in its main loop it will listen for requests to kill via this
function and will forward them to the worker. This is primarily
useful where workers are on a different physical machine to the
controller where \code{\link[tools:pskill]{tools::pskill()}} cannot be used.
}
\examples{
if (redux::redis_available()) {
  rand_str <- function() {
    paste(sample(letters, 20, TRUE), collapse = "")
  }
  # Suppose we have a process that exposes a heartbeat running on
  # this key:
  key <- sprintf("rrq:heartbeat:\%s", rand_str())

  # We can send it a SIGTERM signal over redis using:
  con <- redux::hiredis()
  rrq::rrq_heartbeat_kill(con, key, tools::SIGTERM)
}
}
