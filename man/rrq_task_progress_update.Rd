% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/progress.R
\name{rrq_task_progress_update}
\alias{rrq_task_progress_update}
\title{Post task update}
\usage{
rrq_task_progress_update(value, error = FALSE)
}
\arguments{
\item{value}{An R object with the contents of the update. This
will overwrite any previous progress value, and can be retrieved
by calling \link{rrq_task_progress}.  A value of \code{NULL} will appear
to clear the status, as \code{NULL} will also be returned if no
status is found for a task.}

\item{error}{Logical, indicating if we should throw an error if
not running as an \code{rrq} task. Set this to \code{FALSE} if you want
code to work without modification within and outside of an \code{rrq}
job, or to \code{TRUE} if you want to be sure that progress messages
have made it to the server.}
}
\description{
Post a task progress update.  The progress system in \code{rrq} is
agnostic about how you are going to render your progress, and so
it just a convention - see Details below.  Any R object can be
sent as a progress value (e.g., a string, a list, etc).
}
\details{
In order to report on progress, a task may, in it's code, write

\if{html}{\out{<div class="sourceCode">}}\preformatted{rrq::rrq_task_progress_update("task is 90\% done")
}\if{html}{\out{</div>}}

and this information will be fetchable by calling
\link{rrq_task_progress} with the \code{task_id}.

It is also possible to register progress \emph{without} acquiring
a dependency on \code{rrq}.  If your package/script includes code
like:

\if{html}{\out{<div class="sourceCode">}}\preformatted{progress <- function(message) \{
  signalCondition(structure(list(message = message),
                            class = c("progress", "condition")))
\}
}\if{html}{\out{</div>}}

(this function can be called anything - the important bit is the
body function body - you must return an object with a \code{message}
element and the two class attributes \code{progress} and \code{condition}).

then you can use this in the same way as
\code{rrq::rrq_task_progress_update} above in your code.  When run
without using rrq, this function will appear to do nothing.
}
\examples{
\dontshow{if (rrq:::enable_examples(require_queue = "rrq:example")) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
obj <- rrq_controller("rrq:example")

f <- function(n) {
  for (i in seq_len(n)) {
    rrq::rrq_task_progress_update(sprintf("Iteration \%d / \%d", i, n))
    Sys.sleep(0.1)
  }
  n
}

t <- rrq_task_create_call(f, list(5), controller = obj)
# This might be empty at first
rrq_task_progress(t, controller = obj)
# Wait for the task to complete
rrq_task_wait(t, controller = obj)
# Contains the _last_ progress message
rrq_task_progress(t, controller = obj)
\dontshow{\}) # examplesIf}
}
