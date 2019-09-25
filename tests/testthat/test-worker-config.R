context("worker_config")

test_that("rrq_default configuration", {
  ## (this is actually the *test* default configuration, which is
  ## possibly not what is wanted)
  obj <- test_rrq()
  expect_equal(obj$worker_config_list(), "localhost")
  expect_equal(obj$worker_config_read("localhost"),
               list(time_poll = 1))
})


test_that("create short-lived worker", {
  obj <- test_rrq()

  key <- "stop_immediately"
  cfg <- obj$worker_config_save(key, timeout = 0, time_poll = 1)

  ## Local:
  msg1 <- capture_messages(
    w <- rrq_worker_from_config(obj$keys$queue_name, key))
  msg2 <- capture_messages(res <- w$loop())
  expect_null(res)
  expect_true(any(grepl("STOP OK (TIMEOUT)", msg2, fixed = TRUE)))

  ## Remote:
  logs <- tempfile()
  wid <- test_worker_spawn(obj, worker_config = key, logdir = logs)
  expect_is(wid, "character")
  log <- obj$worker_log_tail(wid, Inf)
  expect_is(log, "data.frame")
  expect_true(nrow(log) >= 1)

  times_up <- queuer:::time_checker(3)
  while (!times_up()) {
    log <- obj$worker_log_tail(wid, Inf)
    if (nrow(log) >= 2L) {
      break
    } else {
      Sys.sleep(0.1)
    }
  }
  expect_equal(nrow(log), 2L)
  expect_equal(log$command[[2]], "STOP")

  logfile <- file.path(logs, wid)
  expect_true(file.exists(logfile))
  txt <- readLines(logfile)
  expect_true(any(grepl("STOP OK (TIMEOUT)", txt, fixed = TRUE)))
})


test_that("Sensible error message on missing config", {
  obj <- test_rrq()

  key <- "nonexistant"

  expect_error(
    rrq_worker_from_config(obj$keys$queue_name, key),
    "Invalid rrq worker configuration key 'nonexistant'")
  expect_error(
    test_worker_spawn(obj, worker_config = key),
    "Invalid rrq worker configuration key 'nonexistant'")
})


test_that("Sensible error if requesting workers on empty key", {
  obj <- test_rrq()
  expect_error(
    worker_wait(obj, "no workers here", timeout = 10, time_poll = 1),
    "No workers expected on that key")
})


test_that("Missing log print fallback", {
  expect_output(
    worker_print_failed_logs(NULL),
    "Logging not enabled for these workers")
})


test_that("worker timeout", {
  obj <- test_rrq("myfuns.R")

  t <- as.integer(runif(1, min = 100, max = 10000))
  res <- obj$worker_config_save("localhost", timeout = t)
  expect_equal(res$timeout, t)

  w <- test_worker_blocking(obj)
  expect_equal(w$timeout, t)
  expect_lte(w$timer(), t)

  id <- obj$message_send("TIMEOUT_GET")
  w$step(TRUE)
  res <- obj$message_get_response(id, w$name)[[1]]
  expect_equal(res[["timeout"]], t)
  expect_lte(res[["remaining"]], t)
})


test_that("infinite timeout", {
  obj <- test_rrq("myfuns.R")
  obj$worker_config_save("infinite", timeout = Inf)

  w <- test_worker_blocking(obj, worker_config = "infinite")
  expect_equal(w$timeout, Inf)
  expect_equal(w$timer(), Inf)

  id <- obj$message_send("TIMEOUT_GET")
  w$step(TRUE)
  res <- obj$message_get_response(id, w$name)[[1]]
  expect_equal(res, c(timeout = Inf, remaining = Inf))
})
