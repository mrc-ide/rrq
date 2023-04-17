## General rrq tests that are slow in this file.

test_that("use task wait timeout when waiting for tasks", {
  obj <- withr::with_options(
    list(rrq.timeout_task_wait = 1),
    test_rrq("myfuns.R"))
  w <- test_worker_spawn(obj)

  t <- obj$enqueue(slowdouble(5))
  err <- expect_error(obj$task_wait(t),
                      "Exceeded maximum time")
  expect_equal(obj$task_wait(t, timeout = 10), 10)
})
