context("rrq")

test_that("sanity checking", {
  root <- tempfile()
  context <- context::context_save(root)

  obj <- rrq_controller(context, redux::hiredis())
  expect_equal(obj$workers_list(), character(0))
  expect_equal(obj$tasks_list(), character(0))
  expect_equal(obj$queue_length(), 0)
  expect_equal(obj$queue_list(), character(0))

  id <- obj$enqueue(sin(1))
  expect_equal(obj$tasks_list(), id)
  expect_equal(obj$queue_list(), id)
  expect_equal(unname(obj$tasks_status(id)), TASK_PENDING)

  test_queue_clean(context$id)
})

test_that("basic use", {
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  ## This needs to send output to a file and not to stdout!
  wid <- worker_spawn(obj$context, obj$con)

  t <- obj$enqueue(slowdouble(0.1))
  expect_is(t, "character")
  expect_equal(obj$task_wait(t, 2), 0.2)
  expect_equal(obj$task_result(t), 0.2)
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

  wid <- worker_spawn(obj$context, obj$con, n_workers, "logs")
  expect_true(all(file.exists(file.path("logs", paste0(wid, ".log")))))

  res <- obj$lapply(x, quote(slowdouble), progress_bar=FALSE)

  expect_equal(res, as.list(x * 2))

  con <- obj$con # save a copy
  obj$destroy()
  on.exit()

  expect_equal(redux::scan_find(con, sprintf("rrq:%s*", context$id)),
               character(0))
})

test_that("worker name", {
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  name <- ids::random_id()
  wid <- worker_spawn(obj$context, obj$con,
                      logdir="logs", worker_name_base=name)
  expect_equal(wid, paste0(name, "_1"))
  expect_true(file.exists(file.path("logs", paste0(name, "_1.log"))))
})

## TODO: in rrqueue, we can register the cluster, and pick up the
## context automatically from environment variables.  Then provide a
## rrq_controller() function that takes no args as a place to start
## from.  That will work pretty well I think.
test_that("exotic functions", {
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  wid <- worker_spawn(context, obj$con, logdir="logs")

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
