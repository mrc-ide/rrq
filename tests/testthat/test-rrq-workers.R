context("workers")

test_that("clean up exited workers", {
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj)

  expect_equal(obj$worker_list(), wid)

  expect_equal(
    obj$message_send_and_wait("STOP"),
    set_names(list("BYE"), wid))

  log <- obj$worker_log_tail(wid, n = Inf)
  expect_equal(log$command, c("ALIVE", "MESSAGE", "RESPONSE", "STOP"))

  expect_equal(obj$worker_list_exited(), wid)
  expect_equal(obj$worker_delete_exited(), wid)
  expect_equal(obj$worker_list_exited(), character(0))
  expect_equal(obj$worker_delete_exited(), character(0))
  expect_error(obj$worker_delete_exited(wid),
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
