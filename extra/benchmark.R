## # rrq benchmarks

## These benchmarks aim to measure the per-call overhead of the
## systems while they do as little as possible.  This is fairly
## meaningless because you'd only typically use parallel execution
## when you have something that takes a significant amount of time,
## but knowing what the overhead is may help determine what
## "significant" means here.

## The `parallel` package will not parallelise anything that uses just
## one core, or just one job so we have to start from two cores and
## two jobs for a fair comparison (otherwise `parallel` stays
## in-process which is way faster).

##+ echo=FALSE, results="hide"
knitr::opts_chunk$set(error = FALSE)

## A simple benchmark attempt:
root <- tempfile()
context <- context::context_load(context::context_save(root))
obj <- rrq::rrq_controller(context, redux::hiredis())

wid <- rrq::worker_spawn(obj, 2L, logdir = tempdir())

## Look in particular at `elapsed`
system.time(obj$lapply(1:1000, identity, progress=FALSE))

## In contrast with the built in socket cluster (though this also pays
## for the `fork()` operation):
system.time(parallel::mclapply(1:1000, identity,
                               mc.preschedule=FALSE, mc.cores=2L))

## Of course, not prescheduling will totally destroy either approach,
## as it has only a single roundtrip.
system.time(parallel::mclapply(1:1000, identity,
                               mc.preschedule=TRUE, mc.cores=2L))

## Let's check this over a matrix of n and workers.  This has to be
## done somewhat annoyingly:
f_rrq <- function(n, nrep, nworkers) {
  if (interactive()) {
    message(sprintf("%d / %d / %d", n, nrep, nworkers))
  }
  diff <- nworkers - obj$worker_len()
  if (diff > 0L) {
    rrq::worker_spawn(obj, diff, logdir=tempdir())
  } else if (diff < 0L) {
    obj$worker_stop(obj$worker_list()[seq_len(-diff)])
  }
  i <- seq_len(n)
  replicate(
    nrep,
    system.time(obj$lapply(i, identity, progress=FALSE))[["elapsed"]])
}
f_mc <- function(n, nrep, nworkers) {
  if (interactive()) {
    message(sprintf("%d / %d / %d", n, nrep, nworkers))
  }
  i <- seq_len(n)
  replicate(
    nrep,
    system.time(parallel::mclapply(i, identity, mc.cores=nworkers,
                                   mc.preschedule=FALSE))[["elapsed"]])
}

n <- round(exp(seq(log(2), log(1024), length.out=7)))
nw <- 2:8
nrep <- 10
dat <- expand.grid(n=n, nrep=nrep, nworkers=nw)

##+ compute_mc
y_mc <-
  vapply(seq_len(nrow(dat)), function(i) do.call(f_mc, dat[i,], quote=TRUE),
         numeric(nrep))

##+ compute_rrq
y_rrq <-
  vapply(seq_len(nrow(dat)), function(i) do.call(f_rrq, dat[i,], quote=TRUE),
         numeric(nrep))

## Convert these into nice matrices, taking the median execution time:
m_mc <- matrix(apply(y_mc, 2, median), length(n))
m_rrq <- matrix(apply(y_rrq, 2, median), length(n))

## Zeros cause trouble later, so set them to the minimum reported time
m_mc[m_mc == 0] <- 0.0001
m_rrq[m_rrq == 0] <- 0.0001

cols <- RColorBrewer::brewer.pal(length(nw), "Blues")

## For multicore, the total time taken is not really affected by the
## number of processors.  There's a very slight cost at low n and a
## very slight gain at high n.
##+ mc_total
ylim <- range(m_mc, m_rrq)
matplot(n, m_mc, type="l", log="xy", lty=1, col=cols, ylim=ylim,
        ylab="parallel, total time")

## With `rrq`, the number of processors does decrease the total
## computation time, though it's very modest.
##+ rrq_total
matplot(n, m_rrq, type="l", log="xy", lty=1, col=cols, ylim=ylim,
        ylab="rrq, total time")
abline(h=max(m_mc), lty=3, col="blue")
abline(h=min(m_mc), lty=3, col="red")

## As the number of elements being parallised over increases, the
## per-element cost shrinks to around 0.001s
##+ mc_percall
ylim <- range(m_mc / n, m_rrq / n)
matplot(n, m_mc / n, type="l", log="xy", lty=1, col=cols, ylim=ylim,
        ylab="parallel, per-call time")

## The per-element floor for `rrq` on one processor is a little
## faster, dropping to ~1/2 this for more than one processor (the
## fastest parallel time is shown in red, slowest in blue)
##+ rrq_percall
matplot(n, m_rrq / n, type="l", log="xy", lty=1, col=cols, ylim=ylim,
        ylab="rrq, per-call time")
abline(h=max(m_mc / n), lty=3, col="blue")
abline(h=min(m_mc / n), lty=3, col="red")

## multicore is faster than `rrq` for small n (probably because there
## is a nontrivial overhead in doing some function checking and
## expression analysis in rrq) but over larger n rrq is faster (above
## about n = 20 here).  With more than one processor, `rrq` is faster
## by a greater margin, and across a wider range of n.
##+ relative
matplot(n, m_rrq / m_mc, type="l", log="x", lty=1, col=cols,
        ylab="rrq time relative to parallel time")
abline(h=1, lty=3, col="red")

obj$destroy()
