test_that("basic use", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t <- rrq_task_create_expr(sin(1), controller = obj)
  w$poll(TRUE)
  worker_run_task_start(w, r6_private(w), t)

  ## Status is NULL by default (actually missing in Redis)
  expect_null(rrq_task_progress(t, controller = obj))

  ## Status can be set
  expect_null(w$progress("status1", TRUE))
  expect_equal(rrq_task_progress(t, controller = obj), "status1")

  ## Status can be updated
  expect_null(w$progress("status2", TRUE))
  expect_equal(rrq_task_progress(t, controller = obj), "status2")

  ## Completing a task retains its status
  worker_run_task(w, r6_private(w), t)
  expect_equal(rrq_task_progress(t, controller = obj), "status2")

  ## Deleting a task removes its status
  rrq_task_delete(t, controller = obj)
  expect_null(rrq_task_progress(t, controller = obj))
})


test_that("update progress from interface function", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_spawn(obj, 1)

  t <- rrq_task_create_expr(run_with_progress(5, 0), controller = obj)
  rrq_task_wait(t, controller = obj)
  expect_equal(rrq_task_result(t, controller = obj), 5)
  expect_equal(rrq_task_progress(t, controller = obj), "iteration 5")
})


test_that("update progress multiple times in task", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_spawn(obj, 1)
  p <- tempfile()

  t <- rrq_task_create_expr(run_with_progress_interactive(p), controller = obj)
  wait_status(t, obj)

  testthat::try_again(5, {
    Sys.sleep(0.5)
    expect_equal(rrq_task_progress(t, controller = obj),
                 "Waiting for file")
  })

  writeLines("something", p)
  Sys.sleep(0.2)
  expect_equal(rrq_task_progress(t, controller = obj),
               "Got contents 'something'")

  writeLines("another thing", p)
  Sys.sleep(0.2)
  expect_equal(rrq_task_progress(t, controller = obj),
               "Got contents 'another thing'")

  writeLines("STOP", p)
  wait_status(t, obj, status = TASK_RUNNING)
  expect_equal(rrq_task_result(t, controller = obj), "OK")
  expect_equal(rrq_task_status(t, controller = obj), TASK_COMPLETE)
  expect_equal(rrq_task_progress(t, controller = obj),
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
  t <- rrq_task_create_expr(run_with_progress_signal(5, 0), controller = obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(t, controller = obj), 5)
  expect_equal(rrq_task_progress(t, controller = obj),
               list(message = "iteration 5"))
})


## TODO: this test is slighly flakey, and I don't see why.
test_that("collect progress from separate process", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_spawn(obj, 1)
  p <- tempfile()

  t <- rrq_task_create_expr(run_with_progress_interactive(p),
                            separate_process = TRUE,
                            controller = obj)
  wait_status(t, obj)

  testthat::try_again(5, {
    Sys.sleep(0.5)
    expect_equal(rrq_task_progress(t, controller = obj),
                 "Waiting for file")
  })

  writeLines("something", p)
  Sys.sleep(0.2)
  expect_equal(rrq_task_progress(t, controller = obj),
               "Got contents 'something'")

  writeLines("another thing", p)
  Sys.sleep(0.2)
  expect_equal(rrq_task_progress(t, controller = obj),
               "Got contents 'another thing'")

  writeLines("STOP", p)
  wait_status(t, obj, status = TASK_RUNNING)
  expect_equal(rrq_task_status(t, controller = obj), TASK_COMPLETE)
  expect_equal(rrq_task_progress(t, controller = obj),
               "Finishing")
})


test_that("collect progress from signal", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  t <- rrq_task_create_expr(run_with_progress_signal(5, 0),
                            separate_process = TRUE,
                            controller = obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(t, controller = obj), 5)
  expect_equal(rrq_task_progress(t, controller = obj),
               list(message = "iteration 5"))
})


## This one actually belongs elsewhere, really.
test_that("Separate process leaves global env clean", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(dirty_double(1))
  t2 <- obj$enqueue(dirty_double(2))
  w$step(TRUE)
  w$step(TRUE)

  ## Running locally pollutes the global environment and is not
  ## isolated:
  expect_equal(rrq_task_result(t1, controller = obj), list(NULL, 2))
  expect_equal(rrq_task_result(t2, controller = obj), list(1, 4))
  expect_equal(.GlobalEnv$.rrq_dirty_double, 2)

  t3 <- obj$enqueue(dirty_double(3), separate_process = TRUE)
  t4 <- obj$enqueue(dirty_double(4), separate_process = TRUE)
  w$step(TRUE)
  w$step(TRUE)

  ## Running in separate process is unaffected by global environment
  ## and does not affect it:
  expect_equal(rrq_task_status(t3, controller = obj), TASK_COMPLETE)
  expect_equal(rrq_task_result(t3, controller = obj), list(NULL, 6))
  expect_equal(rrq_task_result(t4, controller = obj), list(NULL, 8))
  expect_equal(.GlobalEnv$.rrq_dirty_double, 2)

  rm(list = ".rrq_dirty_double", envir = .GlobalEnv)
})
