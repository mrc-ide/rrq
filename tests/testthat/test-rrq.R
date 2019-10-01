context("rrq")

test_that("empty", {
  obj <- test_rrq()

  expect_is(obj, "rrq_controller")

  expect_equal(obj$worker_list(), character(0))
  expect_equal(obj$task_list(), character(0))
  expect_equal(obj$worker_len(), 0)
  expect_equal(obj$queue_length(), 0)
  expect_equal(obj$queue_list(), character(0))

  id <- obj$enqueue(sin(1))
  expect_equal(obj$task_list(), id)
  expect_equal(obj$queue_list(), id)
  expect_equal(obj$task_status(id), setNames(TASK_PENDING, id))

  expect_equal(
    obj$worker_log_tail(),
    data_frame(worker_id = character(0),
               time = numeric(0),
               command = character(0),
               message = character(0)))

  test_queue_clean(obj$queue_id)
})


test_that("basic use", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t <- obj$enqueue(slowdouble(0.1))
  expect_is(t, "character")
  w$step(TRUE)
  expect_equal(obj$task_wait(t, 2, progress = PROGRESS), 0.2)
  expect_equal(obj$task_result(t), 0.2)
})


test_that("task errors are returned", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(only_positive(1))
  w$step(TRUE)
  expect_equal(obj$task_result(t1), 1)

  t2 <- obj$enqueue(only_positive(-1))
  w$step(TRUE)
  res <- obj$task_result(t2)
  expect_is(res, "rrq_task_error")
  expect_null(res$warnings)

  t3 <- obj$enqueue(nonexistant_function(-1))
  w$step(TRUE)
  res <- obj$task_result(t3)
  expect_is(res, "rrq_task_error")
  expect_null(res$warnings)
})


test_that("task warnings are returned", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(warning_then_error(2))
  expect_warning(
    w$step(TRUE),
    "This is warning number \\d")

  r1 <- obj$task_result(t1)
  expect_is(r1, "rrq_task_error")
  expect_is(r1, "try-error")
  expect_is(r1$warnings, "list")
  expect_equal(length(r1$warnings), 2)
  expect_is(r1$warnings[[1]], "simpleWarning")
  expect_equal(r1$warnings[[1]]$message, "This is warning number 1")
  expect_equal(r1$warnings[[2]]$message, "This is warning number 2")

  expect_match(tail(r1$trace, 2)[[1]], "^warning_then_error")
})


test_that("task_position", {
  obj <- test_rrq("myfuns.R")

  t1 <- obj$enqueue(sin(1))
  t2 <- obj$enqueue(sin(1))
  t3 <- obj$enqueue(sin(1))

  expect_equal(obj$task_position(t1), 1L)
  expect_equal(obj$task_position(c(t1, t2, t3)), c(1L, 2L, 3L))
  expect_equal(obj$task_position("not a real task"), 0L)
  expect_equal(obj$task_position("not a real task", NA_integer_), NA_integer_)
  expect_equal(obj$task_position(c(t1, "not a real task"), NA_integer_),
               c(1L, NA_integer_))
})


test_that("task_position", {
  obj <- test_rrq("myfuns.R")

  expect_equal(
    obj$task_overview(),
    list(PENDING = 0, RUNNING = 0, COMPLETE = 0, ERROR = 0))

  t1 <- obj$enqueue(sin(1))
  t2 <- obj$enqueue(sin(1))
  t3 <- obj$enqueue(sin(1))

  expect_equal(
    obj$task_overview(),
    list(PENDING = 3, RUNNING = 0, COMPLETE = 0, ERROR = 0))
})


test_that("wait for tasks without key", {
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj)

  t1 <- obj$enqueue(1 + 1)
  t2 <- obj$enqueue(2 + 2)

  res <- obj$tasks_wait(c(t1, t2))
  expect_equal(res, set_names(list(2, 4), c(t1, t2)))

  ## Slightly slower jobs:
  t3 <- obj$enqueue(slowdouble(0.1))
  t4 <- obj$enqueue(slowdouble(0.1))
  res <- obj$tasks_wait(c(t3, t4))
  expect_equal(res, set_names(list(0.2, 0.2), c(t3, t4)))
})


test_that("wait for tasks with key", {
  obj <- test_rrq("myfuns.R")
  k1 <- rrq_key_task_complete(obj$queue_id)
  t1 <- obj$enqueue(1 + 1, key_complete = k1)
  t2 <- obj$enqueue(2 + 2, key_complete = k1)

  wid <- test_worker_spawn(obj)

  expect_error(
    obj$tasks_wait(c(t1, t2), key_complete = k1, time_poll = 0.1),
    "time_poll must be integer like")
  expect_error(
    obj$tasks_wait(c(t1, t2), key_complete = k1, time_poll = -1),
    "time_poll cannot be less than 1 if using key_complete")
  res <- obj$tasks_wait(c(t1, t2), key_complete = k1)
  expect_equal(res, set_names(list(2, 4), c(t1, t2)))

  ## Slightly slower jobs:
  k2 <- rrq_key_task_complete(obj$queue_id)
  t3 <- obj$enqueue(slowdouble(0.1), key_complete = k2)
  t4 <- obj$enqueue(slowdouble(0.1), key_complete = k2)
  res <- obj$tasks_wait(c(t3, t4), key_complete = k2)
  expect_equal(res, set_names(list(0.2, 0.2), c(t3, t4)))
})


test_that("task delete", {
  obj <- test_rrq("myfuns.R")
  t1 <- obj$enqueue(1 + 1)
  t2 <- obj$enqueue(2 + 2)
  t3 <- obj$enqueue(3 + 3)

  expect_setequal(obj$task_list(), c(t1, t2, t3))
  obj$task_delete(t1)
  expect_setequal(obj$task_list(), c(t2, t3))
  obj$task_delete(c(t2, t3))
  expect_setequal(obj$task_list(), character(0))
})


test_that("wait for tasks on a key", {
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj)
  key <- rrq_key_task_complete(obj$queue_id)

  id <- obj$enqueue(sin(1), key_complete = key)
  res <- obj$task_wait(id, 1, key_complete = NULL, progress = FALSE)
  expect_equal(obj$con$EXISTS(key), 1)
  expect_equal(obj$task_wait(id, key_complete = key), sin(1))
  expect_equal(obj$con$EXISTS(key), 0)
})


test_that("stop worker", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  obj$worker_stop()
  expect_error(
    w$step(), "BYE", class = "rrq_worker_stop")
})


test_that("Can't read logs unless enabled", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_blocking(obj)
  expect_error(
    obj$worker_process_log(w$name),
    "Process log not enabled for this worker")
})


test_that("Can't delete running tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  id <- obj$enqueue(sin(1))
  w$poll(TRUE)
  worker_run_task_start(w, id)
  expect_error(
    obj$task_delete(id),
    "Can't delete running tasks")
})


test_that("Error if results are not ready", {
  obj <- test_rrq()
  id <- obj$enqueue(sin(1))
  expect_error(obj$task_result(id), "Missing some results")
})


test_that("worker load", {
  obj <- test_rrq()
  w1 <- test_worker_blocking(obj)
  w2 <- test_worker_blocking(obj)
  load <- obj$worker_load()
  expect_is(load, "worker_load")
  expect_setequal(load$worker_id, c(w1$name, w2$name))
  avg <- mean(load)
  expect_true(all(avg["used", ] == 0))
  expect_true(all(avg["available", ] == 1))
})


test_that("change environment", {
  create <- function(value) {
    force(value)
    function(envir) {
      envir$x <- value
    }
  }

  obj <- test_rrq()
  obj$envir(create(1))
  w <- test_worker_blocking(obj)
  expect_equal(w$envir$x, 1)

  obj$envir(NULL)
  expect_message(w$step(TRUE), "REFRESH")
  expect_equal(ls(w$envir), character(0))
})


test_that("queue remove", {
  obj <- test_rrq()
  t1 <- obj$enqueue(sin(1))
  t2 <- obj$enqueue(sin(1))
  t3 <- obj$enqueue(sin(1))

  expect_equal(obj$queue_length(), 3)
  expect_equal(obj$queue_remove(t2), TRUE)
  expect_equal(obj$queue_length(), 2)
  expect_equal(obj$queue_list(), c(t1, t3))

  expect_equal(obj$queue_remove(c(t1, t2, t3)), c(TRUE, FALSE, TRUE))
  expect_equal(obj$queue_remove(character(0)), logical(0))
})
