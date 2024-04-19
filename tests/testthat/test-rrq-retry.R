test_that("can retry a task and fetch its status and result", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  ## Run a task once
  set.seed(1)
  t1 <- rrq_task_create_expr(runif(5), controller = obj)
  w$step(TRUE)
  r1 <- rrq_task_result(t1, controller = obj)

  ## Rerun it
  t2 <- rrq_task_retry(t1, controller = obj)
  w$step(TRUE)
  r2 <- rrq_task_result(t2, controller = obj)

  ## Expected output, with same seed, agrees
  set.seed(1)
  cmp1 <- runif(5)
  cmp2 <- runif(5)
  expect_equal(r1, cmp1)
  expect_equal(r2, cmp2)

  ## Now, validate we can follow the status:
  expect_equal(rrq_task_status(t1, follow = FALSE, controller = obj),
               TASK_MOVED)
  expect_equal(rrq_task_status(t1, follow = TRUE, controller = obj),
               TASK_COMPLETE)
  expect_equal(rrq_task_status(t1, controller = obj),
               rrq_task_status(t1, follow = TRUE, controller = obj))

  ## The result
  expect_equal(rrq_task_result(t1, follow = FALSE, controller = obj), r1)
  expect_equal(rrq_task_result(t1, follow = TRUE, controller = obj), r2)
  expect_equal(rrq_task_result(t1, controller = obj),
               rrq_task_result(t1, follow = TRUE, controller = obj))
})


test_that("Can't retry a task that has not been run", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t1 <- rrq_task_create_expr(runif(5), controller = obj)
  t2 <- rrq_task_create_expr(runif(5), controller = obj)
  expect_error(
    rrq_task_retry(t1, controller = obj),
    "Can't retry tasks that are in state: 'PENDING':")
  expect_error(
    rrq_task_retry(c(t1, t2), controller = obj),
    "Can't retry tasks that are in state: 'PENDING':")
  obj$con$HSET(obj$keys$task_status, t1, TASK_RUNNING)
  expect_error(
    rrq_task_retry(c(t1, t2), controller = obj),
    "Can't retry tasks that are in state: 'RUNNING', 'PENDING'")
})


test_that("Can get moved times", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  set.seed(1)
  t1 <- rrq_task_create_expr(runif(5), controller = obj)
  r1 <- rrq_task_times(t1, controller = obj)
  Sys.sleep(0.02)
  w$step(TRUE)
  r1 <- rrq_task_times(t1, controller = obj)
  Sys.sleep(0.02)
  t2 <- rrq_task_retry(t1, controller = obj)
  Sys.sleep(0.02)
  w$step(TRUE)

  ## NOTE: if you print these they don't really look great because
  ## they truncate off the non-integer part which is the only
  ## difference!
  r2 <- rrq_task_times(t2, controller = obj)
  expect_true(all(r2 > r1 | is.na(r2)))

  r3 <- rrq_task_times(t1, controller = obj)
  r4 <- rrq_task_times(t1, follow = TRUE, controller = obj)
  r5 <- rrq_task_times(t1, follow = FALSE, controller = obj)

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
  t1 <- rrq_task_create_expr(runif(5), controller = obj)
  w$step(TRUE)
  t2 <- rrq_task_retry(t1, controller = obj)
  w$step(TRUE)
  t3 <- rrq_task_retry(t2, controller = obj)
  w$step(TRUE)

  info <- rrq_task_info(t1, controller = obj)
  rrq_task_delete(t1, controller = obj)
  expect_equal(rrq_task_list(controller = obj), character(0))
})


test_that("Can delete tasks consistently from leaf", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  set.seed(1)
  t1 <- rrq_task_create_expr(runif(5), controller = obj)
  w$step(TRUE)
  t2 <- rrq_task_retry(t1, controller = obj)
  w$step(TRUE)
  t3 <- rrq_task_retry(t2, controller = obj)
  w$step(TRUE)

  rrq_task_delete(t3, controller = obj)
  expect_equal(rrq_task_list(controller = obj), character(0))
})


test_that("can retry task from non-leaf tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  set.seed(1)
  t1 <- rrq_task_create_expr(runif(5), controller = obj)
  w$step(TRUE)
  t2 <- rrq_task_retry(t1, controller = obj)
  w$step(TRUE)
  t3 <- rrq_task_retry(t1, controller = obj)
  w$step(TRUE)

  expect_equal(rrq_task_info(t1, controller = obj)$moved,
               list(up = NULL, down = c(t2, t3)))
  expect_equal(rrq_task_info(t2, controller = obj)$moved,
               list(up = t1, down = t3))
  expect_equal(rrq_task_info(t3, controller = obj)$moved,
               list(up = c(t1, t2), down = NULL))

  t4 <- rrq_task_retry(t2, controller = obj)
  w$step(TRUE)

  expect_equal(rrq_task_info(t1, controller = obj)$moved,
               list(up = NULL, down = c(t2, t3, t4)))
  expect_equal(rrq_task_info(t2, controller = obj)$moved,
               list(up = t1, down = c(t3, t4)))
  expect_equal(rrq_task_info(t3, controller = obj)$moved,
               list(up = c(t1, t2), down = t4))
  expect_equal(rrq_task_info(t4, controller = obj)$moved,
               list(up = c(t1, t2, t3), down = NULL))
})


test_that("Can't retry duplicate tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t1 <- rrq_task_create_expr(runif(5), controller = obj)
  t2 <- rrq_task_create_expr(runif(5), controller = obj)
  w$step(TRUE)
  w$step(TRUE)

  expect_error(
    rrq_task_retry(c(t1, t1, t1), controller = obj),
    sprintf("task_ids must not contain duplicates:\n  - %s", t1))
  expect_error(
    rrq_task_retry(c(t1, t1, t2, t2), controller = obj),
    sprintf("task_ids must not contain duplicates:\n  - %s\n  - %s", t1, t2))
})


test_that("Can't retry duplicate tasks via redirection", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t1 <- rrq_task_create_expr(runif(5), controller = obj)
  t2 <- rrq_task_create_expr(runif(5), controller = obj)
  t3 <- rrq_task_create_expr(runif(5), controller = obj)
  w$step(TRUE)
  w$step(TRUE)
  w$step(TRUE)

  t4 <- rrq_task_retry(t1, controller = obj)
  t5 <- rrq_task_retry(t2, controller = obj)
  w$step(TRUE)
  w$step(TRUE)

  expect_error(
    rrq_task_retry(c(t1, t4), controller = obj),
    msg <- sprintf(
      "task_ids must point to distinct tasks:\n  - %s\n    - %s\n    - %s",
      t4, t1, t4))
  expect_error(
    rrq_task_retry(c(t1, t2, t3, t4, t5), controller = obj),
    sprintf(
      paste0("task_ids must point to distinct tasks:\n",
             "  - %s\n    - %s\n    - %s\n",
             "  - %s\n    - %s\n    - %s"),
      t4, t1, t4, t5, t2, t5))
})


test_that("Can fetch task data of redirected tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t1 <- rrq_task_create_expr(runif(5), controller = obj)
  w$step(TRUE)
  t2 <- rrq_task_retry(t1, controller = obj)
  expect_identical(rrq_task_data(t2, controller = obj),
                   rrq_task_data(t1, controller = obj))
})


test_that("can follow nested redirect", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  ## Run a task once
  set.seed(1)
  t1 <- rrq_task_create_expr(runif(5), controller = obj)
  w$step(TRUE)
  r1 <- rrq_task_result(t1, controller = obj)
  t2 <- rrq_task_retry(t1, controller = obj)
  w$step(TRUE)
  r2 <- rrq_task_result(t2, controller = obj)
  t3 <- rrq_task_retry(t2, controller = obj)
  w$step(TRUE)
  r3 <- rrq_task_result(t3, controller = obj)

  expect_equal(rrq_task_status(t1, controller = obj), TASK_COMPLETE)
  expect_equal(rrq_task_status(t2, controller = obj), TASK_COMPLETE)
  expect_equal(rrq_task_status(t3, controller = obj), TASK_COMPLETE)

  expect_equal(rrq_task_result(t1, controller = obj), r3)
  expect_equal(rrq_task_result(t2, controller = obj), r3)
  expect_equal(rrq_task_result(t3, controller = obj), r3)
})
