context("progress")

test_that("basic use", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t <- obj$enqueue(sin(1))
  w$poll(TRUE)
  worker_run_task_start(w, r6_private(w), t)

  ## Status is NULL by default (actually missing in Redis)
  expect_null(obj$task_progress(t))

  ## Status can be set
  expect_null(w$progress("status1", TRUE))
  expect_equal(obj$task_progress(t), "status1")

  ## Status can be updated
  expect_null(w$progress("status2", TRUE))
  expect_equal(obj$task_progress(t), "status2")

  ## Completing a task retains its status
  worker_run_task(w, r6_private(w), t)
  expect_equal(obj$task_progress(t), "status2")

  ## Deleting a task removes its status
  obj$task_delete(t)
  expect_null(obj$task_progress(t))
})


test_that("update progress from interface function", {
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj, 1)

  t <- obj$enqueue(run_with_progress(5, 0))
  res <- obj$task_wait(t)
  expect_equal(res, 5)
  expect_equal(obj$task_progress(t), "iteration 5")
})


test_that("update progress multiple times in task", {
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj, 1)
  p <- tempfile()

  t <- obj$enqueue(run_with_progress_interactive(p))
  wait_status(t, obj)

  testthat::try_again(5, {
    Sys.sleep(0.5)
    expect_equal(obj$task_progress(t),
                 "Waiting for file")
  })

  writeLines("something", p)
  Sys.sleep(0.2)
  expect_equal(obj$task_progress(t),
               "Got contents 'something'")

  writeLines("another thing", p)
  Sys.sleep(0.2)
  expect_equal(obj$task_progress(t),
               "Got contents 'another thing'")

  writeLines("STOP", p)
  wait_status(t, obj, status = TASK_RUNNING)
  expect_equal(obj$task_result(t), "OK")
  expect_equal(obj$task_status(t), set_names(TASK_COMPLETE, t))
  expect_equal(obj$task_progress(t),
               "Finishing")
})


test_that("can't register progress with no active worker", {
  cache$active_worker <- NULL
  expect_error(
    rrq_task_progress_update("value", TRUE),
    "rrq_task_progress_update called with no active worker")
  expect_silent(
    rrq_task_progress_update("value", FALSE))
})


test_that("can't register progress with no active task", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  expect_error(
    w$progress("value", TRUE),
    "rrq_task_progress_update called with no active task")
  expect_silent(
    w$progress("value", FALSE))
})


test_that("collect progress from signal", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  t <- obj$enqueue(run_with_progress_signal(5, 0))
  w$step(TRUE)
  expect_equal(obj$task_result(t), 5)
  expect_equal(obj$task_progress(t), list(message = "iteration 5"))
})


## TODO: this test is sli[ghly flakey, and I don't see why.
test_that("collect progress from separate process", {
  skip_if_not_installed("callr")
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj, 1)
  p <- tempfile()

  t <- obj$enqueue(run_with_progress_interactive(p),
                   separate_process = TRUE)
  wait_status(t, obj)

  testthat::try_again(5, {
    Sys.sleep(0.5)
    expect_equal(obj$task_progress(t),
                 "Waiting for file")
  })

  writeLines("something", p)
  Sys.sleep(0.2)
  expect_equal(obj$task_progress(t),
               "Got contents 'something'")

  writeLines("another thing", p)
  Sys.sleep(0.2)
  expect_equal(obj$task_progress(t),
               "Got contents 'another thing'")

  writeLines("STOP", p)
  wait_status(t, obj, status = TASK_RUNNING)
  expect_equal(obj$task_status(t), set_names(TASK_COMPLETE, t))
  expect_equal(obj$task_progress(t),
               "Finishing")
})


test_that("collect progress from signal", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  t <- obj$enqueue(run_with_progress_signal(5, 0),
                   separate_process = TRUE)
  w$step(TRUE)
  expect_equal(obj$task_result(t), 5)
  expect_equal(obj$task_progress(t), list(message = "iteration 5"))
})


test_that("Separate process leaves global env clean", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(dirty_double(1))
  t2 <- obj$enqueue(dirty_double(2))
  w$step(TRUE)
  w$step(TRUE)

  ## Running locally pollutes the global environment and is not
  ## isolated:
  expect_equal(obj$task_result(t1), list(NULL, 2))
  expect_equal(obj$task_result(t2), list(1, 4))
  expect_equal(.GlobalEnv$.rrq_dirty_double, 2)

  t3 <- obj$enqueue(dirty_double(3), separate_process = TRUE)
  t4 <- obj$enqueue(dirty_double(4), separate_process = TRUE)
  w$step(TRUE)
  w$step(TRUE)

  ## Running in separate process is unaffected by global environment
  ## and does not affect it:
  expect_equal(obj$task_status(t3),
               set_names(TASK_COMPLETE, t3))
  expect_equal(obj$task_result(t3), list(NULL, 6))
  expect_equal(obj$task_result(t4), list(NULL, 8))
  expect_equal(.GlobalEnv$.rrq_dirty_double, 2)

  rm(list = ".rrq_dirty_double", envir = .GlobalEnv)
})
