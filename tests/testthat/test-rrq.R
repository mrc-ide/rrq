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
  expect_equal(obj$task_wait(t, 2), 0.2)
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
  expect_is(r1$warnings, "character")
  expect_equal(length(r1$warnings), 2)
  expect_equal(r1$warnings, sprintf("This is warning number %d", 1:2))

  expect_match(r1$trace, "^warning_then_error", all = FALSE)
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


test_that("task_overview", {
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


## This is not ideal, but should do for now. The worker_send_signal
## function is not yet widely used from the controller, but will be at
## some point. We depend on a implementation detail of heartbeatr, but
## it's one that I think I have documented. The alternative would be
## to mock this and ensure that heartbeatr::heartbeat_send_signal is
## called as expected but given how simple the function is it seems
## like the test really just implements the function like that.
test_that("worker_send_signal", {
  obj <- test_rrq()
  w1 <- test_worker_blocking(obj)
  w2 <- test_worker_blocking(obj)
  wid <- c(w1$name, w2$name)
  worker_send_signal(obj$con, obj$keys, tools::SIGINT, c(w1$name, w2$name))

  k <- paste0(rrq_key_worker_heartbeat(obj$keys$queue_id, wid), ":signal")
  expect_equal(obj$con$LINDEX(k[[1]], 0), as.character(tools::SIGINT))
  expect_equal(obj$con$LINDEX(k[[2]], 0), as.character(tools::SIGINT))
})


test_that("cancel queued task", {
  obj <- test_rrq()
  t <- obj$enqueue(sqrt(1))
  expect_true(obj$task_cancel(t))
  expect_equal(obj$task_status(t), setNames(TASK_MISSING, t))
})


test_that("can't cancel completed task", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  t <- obj$enqueue(sqrt(1))
  w$step(TRUE)
  expect_error(
    obj$task_cancel(t),
    "Task [[:xdigit:]]{32} is not running \\(COMPLETE\\)")
})


test_that("can't cancel nonexistant task", {
  obj <- test_rrq()
  id <- ids::random_id()
  expect_error(
    obj$task_cancel(id),
    "Task [[:xdigit:]]{32} is not running \\(MISSING\\)")
})


test_that("get task info", {
  obj <- test_rrq()

  a <- 1
  b <- 2
  t <- obj$enqueue(log(a, b))
  res <- obj$task_data(t)
  expect_equal(res$expr, quote(log(a, b)))
  expect_mapequal(res$objects, list(a = 1, b = 2))
})


test_that("get task data errors appropriately if task is missing", {
  obj <- test_rrq()
  id <- ids::random_id()
  expect_error(
    obj$task_data(id),
    "Task '[[:xdigit:]]+' not found")
})


test_that("a worker will pick up tasks from the priority queue", {
  obj <- test_rrq("myfuns.R")
  obj$worker_config_save("localhost", queue = c("a", "b"))
  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(sin(1))
  t2 <- obj$enqueue(sin(2), queue = "b")
  t3 <- obj$enqueue(sin(3), queue = "a")

  expect_equal(unname(obj$task_status(c(t1, t2, t3))),
               rep("PENDING", 3))
  expect_equal(obj$queue_list(), t1)
  expect_equal(obj$queue_list("b"), t2)
  expect_equal(obj$queue_list("a"), t3)

  w$step(TRUE)
  expect_equal(unname(obj$task_status(c(t1, t2, t3))),
               c("PENDING", "PENDING", "COMPLETE"))
  w$step(TRUE)
  expect_equal(unname(obj$task_status(c(t1, t2, t3))),
               c("PENDING", "COMPLETE", "COMPLETE"))
  w$step(TRUE)
  expect_equal(unname(obj$task_status(c(t1, t2, t3))),
               c("COMPLETE", "COMPLETE", "COMPLETE"))
})


test_that("Query jobs in different queues", {
  obj <- test_rrq("myfuns.R")

  t1 <- obj$enqueue(sin(1), queue = "a")
  t2 <- obj$enqueue(sin(2), queue = "a")
  t3 <- obj$enqueue(sin(3), queue = "a")

  expect_equal(unname(obj$task_status(c(t1, t2, t3))),
               rep("PENDING", 3))
  expect_equal(obj$queue_list(), character(0))
  expect_equal(obj$queue_list("a"), c(t1, t2, t3))

  expect_equal(obj$task_position(t1, queue = "a"), 1)
  expect_equal(obj$task_position(t2, queue = "a"), 2)

  expect_true(obj$queue_remove(t1, queue = "a"))
  expect_false(obj$queue_remove(t1, queue = "a"))

  expect_equal(obj$task_position(t2, queue = "a"), 1)
  expect_equal(obj$queue_length("a"), 2)
  expect_equal(obj$queue_list("a"), c(t2, t3))
})
