context("bulk")

test_that("bulk", {
  Sys.setenv(R_TESTS = "")
  root <- tempfile()
  context <- context::context_save(root, sources = "myfuns.R")
  context <- context::context_load(context, environment())

  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  n_workers <- 5
  wid <- workers_spawn(obj, n_workers, timeout = 5, progress = PROGRESS)

  x <- runif(n_workers * 2) / 10
  res <- obj$lapply(x, quote(slowdouble), progress = PROGRESS)

  expect_equal(res, as.list(x * 2))

  con <- obj$con # save a copy
  obj$destroy()
  on.exit()

  expect_equal(redux::scan_find(con, sprintf("rrq:%s*", context$id)),
               character(0))
})

## TODO: in rrqueue, we can register the cluster, and pick up the
## context automatically from environment variables.  Then provide a
## rrq_controller() function that takes no args as a place to start
## from.  That will work pretty well I think.
test_that("exotic functions", {
  Sys.setenv(R_TESTS = "")
  root <- tempfile()
  context <- context::context_save(root, sources = "myfuns.R")
  context <- context::context_load(context, environment())
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  wid <- workers_spawn(obj, timeout = 5, progress = PROGRESS)
  ## worker_command(obj)

  x <- 1:3
  res <- obj$lapply(x, quote(f1), progress = PROGRESS)
  expect_equal(res, lapply(x, f1))

  res <- local({
    f_local <- function(x) {
      x + 2
    }
    obj$lapply(x, quote(f_local), progress = PROGRESS)
  })
  expect_equal(res, as.list(x + 2))

  res <- local({
    obj$lapply(x, function(x) x + 3, progress = PROGRESS)
  })
  expect_equal(res, as.list(x + 3))
})
