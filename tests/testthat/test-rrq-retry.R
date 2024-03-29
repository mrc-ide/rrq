test_that("can retry a task and fetch its status and result", {
  obj <- test_rrq()
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


test_that("Can get moved times", {
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
  t3 <- obj$task_retry(t2)
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


test_that("Retrying a task is does the right thing with the complete key", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  grp <- obj$lapply(1:3, function(i) runif(1, i, i + 1), timeout_task_wait = 0)
  for (i in 1:3) {
    w$step(TRUE)
  }
  res1 <- obj$bulk_wait(grp, timeout = 1)
  expect_equal(obj$con$LRANGE(grp$key_complete, 0, -1), as.list(grp$task_ids))

  t2 <- obj$task_retry(grp$task_ids[2])
  w$step(TRUE)

  expect_equal(obj$task_status(t2), set_names(TASK_COMPLETE, t2))
  ## We did push the key onto the expected complete list:
  expect_equal(obj$con$LRANGE(grp$key_complete, 0, -1),
               as.list(c(grp$task_ids, t2)))
})


test_that("Pathalogical retry key case is allowed", {
  ## Verifies a fairly unlikely situation where we retry tasks that
  ## were from a bundle (with a dedicated complete key) and free tasks
  ## (without) to make sure that we set these up correctly.
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t1 <- obj$enqueue(runif(1, 3, 4))
  grp <- obj$lapply(2:3, function(i) runif(1, i, i + 1), timeout_task_wait = 0)
  for (i in 1:3) {
    w$step(TRUE)
  }
  res1 <- obj$bulk_wait(grp, timeout = 1)
  expect_equal(
    obj$con$LRANGE(rrq_key_task_complete(obj$queue_id, t1), 0, -1),
    list(t1))
  expect_equal(obj$con$LRANGE(grp$key_complete, 0, -1),
               as.list(grp$task_ids))

  t2 <- obj$task_retry(c(t1, grp$task_ids[2]))
  w$step(TRUE)
  w$step(TRUE)

  expect_equal(
    obj$con$LRANGE(rrq_key_task_complete(obj$queue_id, t2[[1]]), 0, -1),
    list(t2[[1]]))
  expect_equal(obj$con$LRANGE(grp$key_complete, 0, -1),
               as.list(c(grp$task_ids, t2[[2]])))
  expect_equal(obj$task_status(t2), set_names(rep(TASK_COMPLETE, 2), t2))
})


test_that("Can't retry duplicate tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t1 <- obj$enqueue(runif(5))
  t2 <- obj$enqueue(runif(5))
  w$step(TRUE)
  w$step(TRUE)

  expect_error(
    obj$task_retry(c(t1, t1, t1)),
    sprintf("task_ids must not contain duplicates:\n  - %s", t1))
  expect_error(
    obj$task_retry(c(t1, t1, t2, t2)),
    sprintf("task_ids must not contain duplicates:\n  - %s\n  - %s", t1, t2))
})


test_that("Can't retry duplicate tasks via redirection", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t1 <- obj$enqueue(runif(5))
  t2 <- obj$enqueue(runif(5))
  t3 <- obj$enqueue(runif(5))
  w$step(TRUE)
  w$step(TRUE)
  w$step(TRUE)

  t4 <- obj$task_retry(t1)
  t5 <- obj$task_retry(t2)
  w$step(TRUE)
  w$step(TRUE)

  expect_error(
    obj$task_retry(c(t1, t4)),
    msg <- sprintf(
      "task_ids must point to distinct tasks:\n  - %s\n    - %s\n    - %s",
      t4, t1, t4))
  expect_error(
    obj$task_retry(c(t1, t2, t3, t4, t5)),
    sprintf(
      paste0("task_ids must point to distinct tasks:\n",
             "  - %s\n    - %s\n    - %s\n",
             "  - %s\n    - %s\n    - %s"),
      t4, t1, t4, t5, t2, t5))
})


test_that("Can fetch task data of redirected tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t1 <- obj$enqueue(runif(5))
  w$step(TRUE)
  t2 <- obj$task_retry(t1)
  expect_identical(obj$task_data(t2), obj$task_data(t1))
})


test_that("can follow nested redirect", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  ## Run a task once
  set.seed(1)
  t1 <- obj$enqueue(runif(5))
  w$step(TRUE)
  r1 <- obj$task_result(t1)
  t2 <- obj$task_retry(t1)
  w$step(TRUE)
  r2 <- obj$task_result(t2)
  t3 <- obj$task_retry(t2)
  w$step(TRUE)
  r3 <- obj$task_result(t3)

  expect_equal(obj$task_status(t1), setNames(TASK_COMPLETE, t1))
  expect_equal(obj$task_status(t2), setNames(TASK_COMPLETE, t2))
  expect_equal(obj$task_status(t3), setNames(TASK_COMPLETE, t3))

  expect_equal(obj$task_result(t1), r3)
  expect_equal(obj$task_result(t2), r3)
  expect_equal(obj$task_result(t3), r3)
})
