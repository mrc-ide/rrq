test_that("clean up exited workers", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  expect_equal(obj$worker_list(), w$id)

  id <- obj$message_send("STOP")

  expect_error(
    w$step(), "BYE", class = "rrq_worker_stop")
  ## This is typically done by the handler:
  w$shutdown()

  expect_equal(
    obj$message_get_response(id, w$id),
    set_names(list("BYE"), w$id))

  log <- obj$worker_log_tail(w$id, n = Inf)
  expect_equal(log$command, c("ALIVE", "ENVIR", "ENVIR", "QUEUE",
                              "MESSAGE", "RESPONSE", "STOP"))

  expect_equal(obj$worker_list_exited(), w$id)
  expect_equal(obj$worker_delete_exited(), w$id)
  expect_equal(obj$worker_list_exited(), character(0))
  expect_equal(obj$worker_delete_exited(), character(0))
  expect_error(obj$worker_delete_exited(w$id),
               "Workers .+ may not have exited or may not exist")
})


test_that("log parse", {
  worker_id <- "id"
  expect_equal(
    worker_log_parse("123.456 ALIVE", worker_id),
    data_frame(worker_id = worker_id, child = NA_integer_, time = 123.456,
               command = "ALIVE", message = ""))
  expect_equal(
    worker_log_parse("123.456 ALIVE ", worker_id),
    data_frame(worker_id = worker_id, child = NA_integer_, time = 123.456,
               command = "ALIVE", message = ""))
  expect_equal(
    worker_log_parse("123.456 MESSAGE the value", worker_id),
    data_frame(worker_id = worker_id, child = NA_integer_, time = 123.456,
               command = "MESSAGE",
               message = "the value"))
  expect_equal(
    worker_log_parse("123.456/42 ALIVE", worker_id),
    data_frame(worker_id = worker_id, child = 42L, time = 123.456,
               command = "ALIVE", message = ""))
  expect_equal(
    worker_log_parse("123.456/42 ALIVE ", worker_id),
    data_frame(worker_id = worker_id, child = 42L, time = 123.456,
               command = "ALIVE", message = ""))
  expect_equal(
    worker_log_parse("123.456/42 MESSAGE the value", worker_id),
    data_frame(worker_id = worker_id, child = 42L, time = 123.456,
               command = "MESSAGE",
               message = "the value"))
  expect_error(
    worker_log_parse("x123.456 MESSAGE the value", worker_id),
    "Corrupt log")
})


test_that("worker catch graceful stop", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  worker_catch_stop(w, r6_private(w))(rrq_worker_stop_condition(w, "message"))

  expect_false(r6_private(w)$loop_continue)
  log <- obj$worker_log_tail(w$id)
  expect_equal(log$command, "STOP")
  expect_equal(log$message, "OK (message)")
  expect_equal(obj$worker_list(), character(0))
  expect_equal(obj$worker_list_exited(), w$id)
})


test_that("worker catch naked error", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  res <- evaluate_promise(
    expect_error(
      worker_catch_error(w, r6_private(w))(simpleError("Some error")),
      "Some error"))

  expect_match(res$messages,
               "This is an uncaught error in rrq, probably a bug",
               all = FALSE)

  expect_false(r6_private(w)$loop_continue)
  log <- obj$worker_log_tail(w$id)
  expect_equal(log$command, "STOP")
  expect_equal(log$message, "ERROR")
  expect_equal(obj$worker_list(), character(0))
  expect_equal(obj$worker_list_exited(), w$id)
})


test_that("worker ids can't be duplicated", {
  obj <- test_rrq()
  id <- ids::random_id()
  w <- test_worker_blocking(obj, worker_id = id)
  expect_equal(w$id, id)
  expect_error(
    test_worker_blocking(obj, worker_id = id),
    "Looks like this worker exists already",
    fixed = TRUE)
})


test_that("Child workers require a parent", {
  con <- test_hiredis()
  id <- ids::random_id()
  queue_id <- test_name(NULL)
  expect_error(
    rrq_worker$new(queue_id, worker_id = id, is_child = TRUE, con = con),
    "Can't be a child of nonexistant worker")
})


test_that("Timer is recreated after task run", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  obj$message_send("TIMEOUT_SET", 10)
  w$step(TRUE)
  expect_is_function(r6_private(w)$timer)

  obj$enqueue(sin(1))
  w$step(TRUE)
  expect_null(r6_private(w)$timer)

  w$step(TRUE)
  expect_is_function(r6_private(w)$timer)
})


test_that("kill worker with a signal", {
  skip_on_os("windows")

  obj <- test_rrq(timeout_worker_stop = 0)
  cfg <- rrq_worker_config(heartbeat_period = 3)
  res <- obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg)
  w <- test_worker_spawn(obj)

  obj$worker_stop(w$id, "kill")
  Sys.sleep(0.5)
  expect_false(w$is_alive())
})


test_that("kill worker locally", {
  skip_on_os("windows")

  obj <- test_rrq(timeout_worker_stop = 0)
  w <- test_worker_spawn(obj)

  expect_equal(obj$message_send_and_wait("PING", w$id)[[1]], "PONG")

  ## Can't kill with heartbeat:
  expect_error(
    obj$worker_stop(w$id, "kill"),
    "Worker does not support heatbeat - can't kill with signal:")
  expect_true(w$is_alive())

  ## But can kill locally:
  obj$worker_stop(w$id, "kill_local")
  Sys.sleep(0.5)
  expect_false(w$is_alive())
})


test_that("Can't kill non-local workers", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  info <- obj$worker_info(w$id)[[w$id]]
  info$hostname <- paste0(info$hostname, "_but_on_mars")
  obj$con$HSET(queue_keys(obj)$worker_info, w$id, object_to_bin(info))
  expect_error(
    obj$worker_stop(w$id, "kill_local"),
    "Not all workers are local:")
})


test_that("rrq_worker_main_args parse", {
  expect_equal(
    rrq_worker_main_args("name"),
    list(queue_id = "name",
         config = WORKER_CONFIG_DEFAULT,
         worker_id = NULL))

  expect_equal(
    rrq_worker_main_args(c("name", "--worker-id=bob")),
    list(queue_id = "name",
         config = WORKER_CONFIG_DEFAULT,
         worker_id = "bob"))

  expect_equal(
    rrq_worker_main_args(c("name", "--config=config")),
    list(queue_id = "name",
         config = "config",
         worker_id = NULL))
})


test_that("write worker script", {
  p <- tempfile()
  res <- rrq_worker_script(p)
  expect_equal(normalizePath(dirname(res)), normalizePath(p))
  expect_equal(basename(res), "rrq_worker")
  expect_equal(readLines(res)[[1]], "#!/usr/bin/env Rscript")
})


test_that("write versioned worker script", {
  skip_on_cran()
  res <- rrq_worker_script(tempfile(), versioned = TRUE)
  expect_match(readLines(res)[[1]], R.home(), fixed = TRUE)
})


test_that("write_rrq_workers is deprecated", {
  skip_if_not_installed("mockery")
  mock_worker_script <- mockery::mock()
  mockery::stub(write_rrq_worker, "rrq_worker_script", mock_worker_script)
  path <- tempfile()
  expect_warning(write_rrq_worker(path)) # deprecation warning
  mockery::expect_called(mock_worker_script, 1)
  expect_equal(mockery::mock_args(mock_worker_script)[[1]], list(path))
})


## this is a regression test for
## https://github.com/mrc-ide/rrq/issues/56
test_that("clean up worker when one still running", {
  obj <- test_rrq()
  w1 <- test_worker_blocking(obj)
  w2 <- test_worker_blocking(obj)

  expect_setequal(obj$worker_list(), c(w1$id, w2$id))

  id <- obj$message_send("STOP", worker_ids = w1$id)

  expect_error(
    w1$step(), "BYE", class = "rrq_worker_stop")

  ## This is typically done by the handler:
  w1$shutdown()

  expect_equal(
    obj$message_get_response(id, w1$id),
    set_names(list("BYE"), w1$id))

  expect_equal(obj$worker_list_exited(), w1$id)

  expect_equal(obj$worker_delete_exited(), w1$id)
  expect_setequal(obj$worker_list(), w2$id)
  expect_equal(obj$worker_list_exited(), character(0))
  expect_equal(obj$worker_delete_exited(), character(0))
})


test_that("can get worker info", {
  skip_on_os("windows")

  obj <- test_rrq(timeout_worker_stop = 10)
  cfg <- rrq_worker_config(heartbeat_period = 3)
  res <- obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg)
  w <- test_worker_spawn(obj)

  info <- obj$worker_info(w$id)
  expect_length(info, 1)
  expect_s3_class(info[[1]], "rrq_worker_info")
  expect_equal(names(info), w$id)
  expect_setequal(names(info[[w$id]]),
                  c("worker", "config", "rrq_version", "platform", "running",
                    "hostname",
                    "username", "queue", "wd", "pid", "redis_host",
                    "redis_port", "heartbeat_key"))
  expect_equal(info[[w$id]]$rrq_version, version_info())
})


test_that("multiple queues format correctly when printing worker", {
  obj <- test_rrq()
  cfg <- rrq_worker_config(queue = c("a", "b"), verbose = FALSE)
  obj$worker_config_save(WORKER_CONFIG_DEFAULT, cfg)
  w <- test_worker_blocking(obj)
  format <- worker_format(w)

  ## Find queue string
  queue_string <- grep("queue:", format, value = TRUE)
  queue_string_split <- strsplit(queue_string, "\n")[[1]]
  ## Three strings, one per queue:
  expect_length(queue_string_split, 3)
  ## All strings queue strings start at the same length:
  loc <- regexpr(obj$queue_id, queue_string_split)
  expect_length(unique(loc), 1)
  expect_equal(sub(".+:queue:", "", queue_string_split),
               c("a", "b", "default"))
})
