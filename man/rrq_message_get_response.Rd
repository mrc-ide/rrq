% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rrq_message.R
\name{rrq_message_get_response}
\alias{rrq_message_get_response}
\title{Get message response}
\usage{
rrq_message_get_response(
  message_id,
  worker_ids = NULL,
  named = TRUE,
  delete = FALSE,
  timeout = 0,
  time_poll = 0.5,
  progress = NULL,
  controller = NULL
)
}
\arguments{
\item{message_id}{The message id}

\item{worker_ids}{Optional vector of worker ids. If \code{NULL} then
all active workers are used (note that this may differ to the
set of workers that the message was sent to!)}

\item{named}{Logical, indicating if the return value should be
named by worker id.}

\item{delete}{Logical, indicating if messages should be deleted
after retrieval}

\item{timeout}{Integer, representing seconds to wait until the
response has been received. An error will be thrown if a
response has not been received in this time.}

\item{time_poll}{If \code{timeout} is greater than zero, this is the
polling interval used between redis calls.  Increasing this
reduces network load but increases the time that may be waited
for.}

\item{progress}{Optional logical indicating if a progress bar
should be displayed. If \code{NULL} we fall back on the value of the
global option \code{rrq.progress}, and if that is unset display a
progress bar if in an interactive session.}

\item{controller}{The controller to use.  If not given (or \code{NULL})
we'll use the controller registered with
\code{\link[=rrq_default_controller_set]{rrq_default_controller_set()}}.}
}
\description{
Get response to messages, waiting until the message has been
responded to.
}
\examples{
\dontshow{if (rrq:::enable_examples(require_queue = "rrq:example")) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
obj <- rrq_controller("rrq:example")
id <- rrq_message_send("PING", controller = obj)
rrq_message_get_response(id, timeout = 5, controller = obj)
\dontshow{\}) # examplesIf}
}
