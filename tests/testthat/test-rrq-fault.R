test_that("can retry a task and fetch its status and result", {
  obj <- test_rrq()
  ## obj$worker_config_save("localhost", verbose = TRUE)
  w <- test_worker_blocking(obj)

  ## Run a task once
  set.seed(1)
  t1 <- obj$enqueue(runif(5))
  w$step(TRUE)
  r1 <- obj$task_result(t1)

  ## Rerun it
  t2 <- obj$task_retry(t1)
  w$step(TRUE)
  r2 <- obj$task_result(t2)

  ## Expected output, with same seed, agrees
  set.seed(1)
  cmp1 <- runif(5)
  cmp2 <- runif(5)
  expect_equal(r1, cmp1)
  expect_equal(r2, cmp2)

  ## Now, validate we can follow the status:
  expect_equal(obj$task_status(t1, follow = FALSE), set_names(TASK_MOVED, t1))
  expect_equal(obj$task_status(t1, follow = TRUE), set_names(TASK_COMPLETE, t1))
  expect_equal(obj$task_status(t1), obj$task_status(t1, follow = TRUE))

  ## The result
  expect_equal(obj$task_result(t1, follow = FALSE), r1)
  expect_equal(obj$task_result(t1, follow = TRUE), r2)
  expect_equal(obj$task_result(t1), obj$task_result(t1, follow = TRUE))
})


test_that("Can't retry a task that has not been run", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t1 <- obj$enqueue(runif(5))
  t2 <- obj$enqueue(runif(5))
  expect_error(
    obj$task_retry(t1),
    "Can't retry tasks that are in state: 'PENDING':")
  expect_error(
    obj$task_retry(c(t1, t2)),
    "Can't retry tasks that are in state: 'PENDING':")
  obj$con$HSET(queue_keys(obj)$task_status, t1, TASK_RUNNING)
  expect_error(
    obj$task_retry(c(t1, t2)),
    "Can't retry tasks that are in state: 'RUNNING', 'PENDING'")
})


test_that("Can get deferred times", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  set.seed(1)
  t1 <- obj$enqueue(runif(5))
  r1 <- obj$task_times(t1)
  Sys.sleep(0.02)
  w$step(TRUE)
  r1 <- obj$task_times(t1)
  Sys.sleep(0.02)
  t2 <- obj$task_retry(t1)
  Sys.sleep(0.02)
  w$step(TRUE)

  ## NOTE: if you print these they don't really look great because
  ## they truncate off the non-integer part which is the only
  ## difference!
  r2 <- obj$task_times(t2)
  expect_true(all(r2 > r1 | is.na(r2)))

  r3 <- obj$task_times(t1)
  r4 <- obj$task_times(t1, follow = TRUE)
  r5 <- obj$task_times(t1, follow = FALSE)

  expect_equal(r5[1:3], r1[1:3])
  expect_true(is.na(r1[, "moved"]))
  expect_false(is.na(r5[, "moved"]))

  expect_equal(r3, r4)
  expect_equal(unname(r2), unname(r3))
  expect_equal(rownames(r2), t2)
  expect_equal(rownames(r3), t1)
})


test_that("Can delete tasks consistently from root", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  set.seed(1)
  t1 <- obj$enqueue(runif(5))
  w$step(TRUE)
  t2 <- obj$task_retry(t1)
  w$step(TRUE)
  t3 <- obj$task_retry(t2) # TODO: broken if given t1, failing to follow
  w$step(TRUE)

  info <- obj$task_info(t1)
  obj$task_delete(t1)
  expect_equal(obj$task_list(), character(0))
})


test_that("Can delete tasks consistently from leaf", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  set.seed(1)
  t1 <- obj$enqueue(runif(5))
  w$step(TRUE)
  t2 <- obj$task_retry(t1)
  w$step(TRUE)
  t3 <- obj$task_retry(t2)
  w$step(TRUE)

  obj$task_delete(t3)
  expect_equal(obj$task_list(), character(0))
})


test_that("can retry task from non-leaf tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  set.seed(1)
  t1 <- obj$enqueue(runif(5))
  w$step(TRUE)
  t2 <- obj$task_retry(t1)
  w$step(TRUE)
  t3 <- obj$task_retry(t1)
  w$step(TRUE)

  expect_equal(obj$task_info(t1)$moved, list(up = NULL, down = c(t2, t3)))
  expect_equal(obj$task_info(t2)$moved, list(up = t1, down = t3))
  expect_equal(obj$task_info(t3)$moved, list(up = c(t1, t2), down = NULL))

  t4 <- obj$task_retry(t2)
  w$step(TRUE)

  expect_equal(obj$task_info(t1)$moved, list(up = NULL, down = c(t2, t3, t4)))
  expect_equal(obj$task_info(t2)$moved, list(up = t1, down = c(t3, t4)))
  expect_equal(obj$task_info(t3)$moved, list(up = c(t1, t2), down = t4))
  expect_equal(obj$task_info(t4)$moved, list(up = c(t1, t2, t3), down = NULL))
})
