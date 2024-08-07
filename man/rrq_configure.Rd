% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/configure.R
\name{rrq_configure}
\alias{rrq_configure}
\title{Configure rrq}
\usage{
rrq_configure(
  queue_id,
  con = redux::hiredis(),
  ...,
  store_max_size = Inf,
  offload_path = NULL
)
}
\arguments{
\item{queue_id}{The queue id; the same as you would pass to
\link{rrq_controller}}

\item{con}{A redis connection}

\item{...}{Additional arguments - this must be empty. This
argument exists so that all additional arguments must be passed
by name.}

\item{store_max_size}{The maximum object size, in bytes, before
being moved to the offload store. If given, then larger data
will be saved in \code{offload_path} (using
\code{\link{object_store_offload_disk}})}

\item{offload_path}{The path to create an offload store at (passed
to \code{\link{object_store_offload_disk}}). The directory will be
created if it does not exist. If not given (or \code{NULL}) but
\code{store_max_size} is finite, then trying to save large objects
will throw an error.}
}
\value{
Invisibly, a list with processed configuration information
}
\description{
Configure rrq options. This function must be called before either
a controller or worker connects to a queue, as the options will
apply to both. The function may only be called once on a given
queue as there is no facility (yet) to update options. Currently
the options concern only storage, and specifically how larger
objects will be saved (using \code{\link{object_store}}.
}
\section{Storage}{


Every time that a task is saved, or a task is completed, results
are saved into the Redis database. Because Redis is an in-memory
database, it's not a great idea to save very large objects into
it (if you ran 100 jobs in parallel and each saved a 2GB object
you'd likely take down your redis server). In addition, \code{redux}
does not support directly saving objects larger than \code{2^31 - 1}
bytes into Redis. So, for some use cases we need to consider
where to store larger objects.

The strategy here is to "offload" the larger objects - bigger than
some user-given size - onto some other storage system. Currently
the only alternative supported is a disk store
(\code{\link{object_store_offload_disk}}) but we hope to expand this
later.  So if your task returns a 3GB object then we will spill
that to disk rather than failing to save that into
Redis.

How big is an object? We serialise the object
(\code{redux::object_to_bin} just wraps \code{\link{serialize}}) which creates
a vector of bytes and that is saved into the database. To get an
idea of how large things are you can do:
\code{length(redux::object_to_bin(your_object))}.  At the time this
documentation was written, \code{mtcars} was \code{3807} bytes, and a
million random numbers was \verb{8,000,031} bytes. It's unlikely that
a \code{store_max_size} of less than 1MB will be sensible.
}

\examples{
\dontshow{if (rrq:::enable_examples()) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
tmp <- tempfile()
dir.create(tmp)
rrq_configure("rrq:offload", store_max_size = 100000, offload_path = tmp)
obj <- rrq_controller("rrq:offload")
x <- runif(100000)
t <- rrq_task_create_expr(mean(x), controller = obj)
dir(tmp)
file.size(dir(tmp, full.names = TRUE))
rrq_destroy(controller = obj)
\dontshow{\}) # examplesIf}
}
