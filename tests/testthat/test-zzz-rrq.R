## General rrq tests that are slow in this file.

test_that("use task wait timeout when waiting for tasks", {
  obj <- withr::with_options(
    list(rrq.timeout_task_wait = 1),
    test_rrq("myfuns.R"))
  w <- test_worker_spawn(obj)

  t <- obj$enqueue(slowdouble(5))
  err <- expect_error(rrq_task_wait(t, controller = obj),
                      "task did not complete in time")
  expect_true(rrq_task_wait(t, timeout = 10, controller = obj))
  expect_equal(rrq_task_result(t, controller = obj), 10)
})
