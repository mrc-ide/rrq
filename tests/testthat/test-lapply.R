context("lapply")

test_that("local variables", {
  root <- tempfile()
  context <- context::context_save(root, envir=.GlobalEnv)
  env <- context::context_load(context, FALSE)
  obj <- rrq_controller(context, redux::hiredis(), env)
  on.exit(obj$destroy())
  e <- environment()

  expect_error(rrq_lapply_submit(obj, 1:5, list, a, envir=e),
               "not all objects found: a")

  a <- 1
  res <- rrq_lapply_submit(obj, 1:5, list, a, envir=e)
  t <- res$task_ids[[1]]
  dat <- redux::bin_to_object(obj$con$HGET(obj$keys$tasks_expr, t))
  expect_equal(dat$objects, c(a=digest::digest(a)))
  expect_equal(dat$expr, quote(base::list(1L, a)))
})

test_that("bulk", {
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  env <- context::context_load(context, FALSE)
  obj <- rrq_controller(context, redux::hiredis(), env)
  on.exit(obj$destroy())

  n_workers <- 5
  x <- runif(n_workers * 2)

  wid <- workers_spawn(obj$context, obj$con, n_workers, "logs")
  expect_true(all(file.exists(file.path("logs", paste0(wid, ".log")))))

  res <- obj$lapply(x, quote(slowdouble), progress_bar=FALSE)

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
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  env <- context::context_load(context, FALSE)
  obj <- rrq_controller(context, redux::hiredis(), env)
  on.exit(obj$destroy())

  wid <- workers_spawn(context, obj$con, logdir="logs")
  ## worker_command(obj)
  
  x <- 1:3
  res <- obj$lapply(x, quote(f1), progress_bar=FALSE)
  expect_equal(res, lapply(x, f1))

  res <- local({
    f_local <- function(x) {
      x + 2
    }
    obj$lapply(x, quote(f_local), progress_bar=FALSE)
  })
  expect_equal(res, as.list(x + 2))

  res <- local({
    obj$lapply(x, function(x) x + 3, progress_bar=FALSE)
  })
  expect_equal(res, as.list(x + 3))
})
