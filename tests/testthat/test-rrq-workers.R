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
    "REQUEUE queue:")

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


test_that("create worker", {
  obj <- test_rrq()
  name <- ids::random_id()
  w <- rrq_worker(obj$keys$queue_name, worker_name = name,
                  timeout = 1, time_poll = 1)
  log <- obj$worker_log_tail(name, Inf)
  expect_equal(log$command, c("ALIVE", "STOP"))
})


test_that("create parallel worker", {
  Sys.setenv("CONTEXT_CORES" = 1)
  on.exit(Sys.unsetenv("CONTEXT_CORES"))
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  expect_equal(w$cores, 1)
  expect_is(context::parallel_cluster(), "cluster")
  w$shutdown()
  expect_error(context::parallel_cluster(),
               "Cluster has not been started yet")
})


test_that("worker names can't be duplicated", {
  obj <- test_rrq()
  name <- ids::random_id()
  w <- test_worker_blocking(obj, worker_name = name)
  expect_equal(w$name, name)
  expect_error(
    test_worker_blocking(obj, worker_name = name),
    "Looks like this worker exists already...",
    fixed = TRUE)
})


test_that("log path must be relative", {
  context <- test_context()
  path_logs <- file.path(context$root$path, "logs")
  expect_error(
    worker_initialise_logs(context, path_logs),
    "Must be a relative path")
  expect_silent(worker_initialise_logs(context, "logs"))
  expect_true(file.exists(path_logs))
})


test_that("Timer is recreated after task run", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  obj$message_send("TIMEOUT_SET", 10)
  w$step(TRUE)
  expect_is(w$timer, "function")

  obj$enqueue(sin(1))
  w$step(TRUE)
  expect_null(w$timer)

  w$step(TRUE)
  expect_is(w$timer, "function")
})


test_that("kill worker with a signal", {
  skip_if_not_installed("heartbeatr")
  skip_on_os("windows")

  obj <- test_rrq()
  res <- obj$worker_config_save("localhost", heartbeat_period = 3)
  wid <- test_worker_spawn(obj)

  pid <- obj$worker_info(wid)[[1]]$pid
  alive_value <- tools::pskill(pid, 0)
  expect_equal(obj$message_send_and_wait("PING", wid)[[1]], "PONG")

  obj$worker_stop(wid, "kill")
  Sys.sleep(0.5)
  expect_false(tools::pskill(pid, 0) == alive_value)
})


test_that("kill worker locally", {
  skip_on_os("windows")

  obj <- test_rrq()
  wid <- test_worker_spawn(obj)

  pid <- obj$worker_info(wid)[[1]]$pid
  alive_value <- tools::pskill(pid, 0)
  expect_equal(obj$message_send_and_wait("PING", wid)[[1]], "PONG")

  ## Can't kill with heartbeat:
  expect_error(
    obj$worker_stop(wid, "kill"),
    "Worker does not support heatbeat - can't kill with signal:")

  ## But can kill locally:
  obj$worker_stop(wid, "kill_local")
  Sys.sleep(0.5)
  expect_false(tools::pskill(pid, 0) == alive_value)
})


test_that("Can't kill non-local workers", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  info <- obj$worker_info(w$name)[[w$name]]
  info$hostname <- paste0(info$hostname, "_but_on_mars")
  obj$con$HSET(obj$keys$worker_info, w$name, object_to_bin(info))
  expect_error(
    obj$worker_stop(w$name, "kill_local"),
    "Not all workers are local:")
})


test_that("rrq_worker_main_args parse", {
  expect_equal(
    rrq_worker_main_args("name"),
    list(queue_id = "name",
         config = "localhost",
         name = NULL,
         key_alive = NULL))

  expect_equal(
    rrq_worker_main_args(c("name", "--name=bob")),
    list(queue_id = "name",
         config = "localhost",
         name = "bob",
         key_alive = NULL))

  expect_equal(
    rrq_worker_main_args(c("name", "--key-alive=key")),
    list(queue_id = "name",
         config = "localhost",
         name = NULL,
         key_alive = "key"))

  expect_equal(
    rrq_worker_main_args(c("name", "--config=config")),
    list(queue_id = "name",
         config = "config",
         name = NULL,
         key_alive = NULL))
})


test_that("write worker script", {
  p <- tempfile()
  res <- write_rrq_worker(p)
  expect_equal(dirname(res), p)
  expect_equal(basename(res), "rrq_worker")
  expect_equal(readLines(res)[[1]], "#!/usr/bin/env Rscript")
})


test_that("write versioned worker script", {
  res <- write_rrq_worker(versioned = TRUE)
  expect_match(readLines(res)[[1]], R.home(), fixed = TRUE)
})
