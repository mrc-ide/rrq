context("bulk")


## These check that tasks can be waited on
test_that("bulk", {
  prev <- character(0)
  obj <- test_rrq("myfuns.R")
  envir <- obj$context$envir
  n_workers <- 5
  wid <- test_worker_spawn(obj, n_workers)

  x <- runif(n_workers * 2) / 10
  res <- obj$lapply(x, quote(slowdouble), progress = PROGRESS,
                    envir = envir, timeout = 10)

  expect_equal(res, as.list(x * 2))

  con <- obj$con # save a copy
  id <- obj$context$id
  obj$destroy(worker_stop_timeout = 0.5)

  expect_equal(
    redux::scan_find(con, sprintf("rrq:%s*", id)),
    character(0))
})


test_that("exotic functions", {
  obj <- test_rrq("myfuns.R")
  envir <- obj$context$envir

  wid <- test_worker_spawn(obj)

  x <- 1:3
  res <- obj$lapply(x, quote(f1), progress = PROGRESS, envir = envir,
                    timeout = 2)
  expect_equal(res, lapply(x, envir$f1))

  res <- local({
    f_local <- function(x) {
      x + 2
    }
    obj$lapply(x, quote(f_local), progress = PROGRESS, timeout = 10)
  })
  expect_equal(res, as.list(x + 2))

  res <- local({
    obj$lapply(x, function(x) x + 3, progress = PROGRESS, timeout = 10)
  })
  expect_equal(res, as.list(x + 3))
})
