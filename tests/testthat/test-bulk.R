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
  res1 <- obj$lapply(x, quote(f1), progress = PROGRESS, envir = envir,
                    timeout = 2)
  expect_equal(res1, lapply(x, envir$f1))
  res2 <- obj$enqueue_bulk(x, quote(f1), progress = PROGRESS, envir = envir,
                          timeout = 2)
  expect_equal(res1, res2)

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


test_that("lapply requires a loaded context", {
  obj <- test_rrq("myfuns.R")
  rrq <- rrq_controller(obj$context$id, obj$con)
  expect_error(
    rrq$lapply(1:3, quote(f1), progress = FALSE),
    "lapply requires a loaded context")
  expect_error(
    rrq$enqueue_bulk(1:3, quote(f1), progress = FALSE),
    "enqueue_bulk requires a loaded context")
})
