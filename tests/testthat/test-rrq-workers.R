context("workers")

test_that("clean up exited workers", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  expect_equal(obj$worker_list(), w$name)

  id <- obj$message_send("STOP")

  expect_error(
    w$step(), "BYE", class = "rrq_worker_stop")
  ## This is typically done by the handler:
  w$shutdown()

  expect_equal(
    obj$message_get_response(id, w$name),
    set_names(list("BYE"), w$name))

  log <- obj$worker_log_tail(w$name, n = Inf)
  expect_equal(log$command, c("ALIVE", "MESSAGE", "RESPONSE", "STOP"))

  expect_equal(obj$worker_list_exited(), w$name)
  expect_equal(obj$worker_delete_exited(), w$name)
  expect_equal(obj$worker_list_exited(), character(0))
  expect_equal(obj$worker_delete_exited(), character(0))
  expect_error(obj$worker_delete_exited(w$name),
               "Workers .+ may not have exited or may not exist")
})


test_that("log parse", {
  worker_id <- "id"
  expect_equal(
    worker_log_parse("123.456 ALIVE", worker_id),
    data_frame(worker_id = worker_id, time = 123.456, command = "ALIVE",
               message = ""))
  expect_equal(
    worker_log_parse("123.456 ALIVE ", worker_id),
    data_frame(worker_id = worker_id, time = 123.456, command = "ALIVE",
               message = ""))
  expect_equal(
    worker_log_parse("123.456 MESSAGE the value", worker_id),
    data_frame(worker_id = worker_id, time = 123.456, command = "MESSAGE",
               message = "the value"))
  expect_error(
    worker_log_parse("x123.456 MESSAGE the value", worker_id),
    "Corrupt log")
})


test_that("worker catch gracefull stop", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  worker_catch_stop(w)(rrq_worker_stop(w, "message"))

  expect_false(w$loop_continue)
  log <- obj$worker_log_tail(w$name)
  expect_equal(log$command, "STOP")
  expect_equal(log$message, "OK (message)")
  expect_equal(obj$worker_list(), character(0))
  expect_equal(obj$worker_list_exited(), w$name)
})


test_that("worker catch naked error", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  res <- evaluate_promise(
    expect_error(
      worker_catch_error(w)(simpleError("Some error")),
      "Some error"))

  expect_match(res$messages,
               "This is an uncaught error in rrq, probably a bug",
               all = FALSE)

  expect_false(w$loop_continue)
  log <- obj$worker_log_tail(w$name)
  expect_equal(log$command, "STOP")
  expect_equal(log$message, "ERROR")
  expect_equal(obj$worker_list(), character(0))
  expect_equal(obj$worker_list_exited(), w$name)
})


test_that("worker catch interrupt on an idle worker", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  e <- structure(list(), class = c("interrupt", "condition"))
  expect_message(
    worker_catch_interrupt(w)(interrupt()),
    "INTERRUPT")
})


test_that("worker catch interrupt with collected, but unstarted, task", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id1 <- obj$enqueue(sin(1))
  id2 <- obj$enqueue(sin(2))
  expect_equal(obj$queue_list(), c(id1, id2))

  w$poll(TRUE)
  expect_equal(obj$queue_list(), id2)
  id3 <- obj$enqueue(sin(3))
  expect_equal(obj$queue_list(), c(id2, id3))

  expect_message(
    worker_catch_interrupt(w)(interrupt()),
    "REQUEUE rrq:")

  expect_equal(obj$queue_list(), c(id1, id2, id3))
})


test_that("worker catch interrupt with started task", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id1 <- obj$enqueue(sin(1))
  w$poll(TRUE)
  worker_run_task_start(w, id1)

  expect_equal(obj$task_status(id1), set_names(TASK_RUNNING, id1))

  expect_message(
    worker_catch_interrupt(w)(interrupt()),
    "TASK_INTERRUPTED")

  expect_equal(obj$task_status(id1), set_names(TASK_INTERRUPTED, id1))
  expect_equal(obj$worker_status(w$name), set_names(WORKER_IDLE, w$name))
  expect_equal(obj$queue_list(), character(0))
})
