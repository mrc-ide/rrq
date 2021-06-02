test_that("rrq_default configuration", {
  ## (this is actually the *test* default configuration, which is
  ## possibly not what is wanted)
  obj <- test_rrq()
  expect_equal(obj$worker_config_list(), "localhost")
  expect_equal(obj$worker_config_read("localhost"),
               list(time_poll = 1, verbose = TRUE))
})


test_that("create short-lived worker", {
  obj <- test_rrq()

  key <- "stop_immediately"
  cfg <- obj$worker_config_save(key, timeout = 0, time_poll = 1)

  ## Local:
  msg1 <- capture_messages(
    w <- rrq_worker_from_config(obj$queue_id, key))
  msg2 <- capture_messages(res <- w$loop())
  expect_null(res)
  expect_true(any(grepl("STOP OK (TIMEOUT)", msg2, fixed = TRUE)))

  ## Remote:
  logs <- tempfile()
  wid <- test_worker_spawn(obj, worker_config = key, logdir = logs)
  expect_type(wid, "character")
  log <- obj$worker_log_tail(wid, Inf)
  expect_s3_class(log, "data.frame")
  expect_true(nrow(log) >= 1)

  remaining <- time_checker(3)
  while (remaining() > 0) {
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
    rrq_worker_from_config(obj$queue_id, key),
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
  expect_equal(r6_private(w)$timeout, t)
  expect_lte(r6_private(w)$timer(), t)

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
  expect_equal(r6_private(w)$timeout, Inf)
  expect_equal(r6_private(w)$timer(), Inf)

  id <- obj$message_send("TIMEOUT_GET")
  w$step(TRUE)
  res <- obj$message_get_response(id, w$name)[[1]]
  expect_equal(res, c(timeout = Inf, remaining = Inf))
})



test_that("rrq_default configuration", {
  ## (this is actually the *test* default configuration, which is
  ## possibly not what is wanted)
  obj <- test_rrq()
  res1 <- obj$worker_config_save("new", timeout = 1, overwrite = FALSE)
  expect_equal(res1, list(timeout = 1, verbose = TRUE))
  res2 <- obj$worker_config_save("new", timeout = 2, overwrite = FALSE)
  expect_null(res2)
  expect_equal(obj$worker_config_read("new"), res1)
})


test_that("verbose is validated", {
  obj <- test_rrq()
  expect_error(
    obj$worker_config_save("quiet", verbose = "no thank you"),
    "verbose must be logical")
  obj$worker_config_save("quiet", verbose = FALSE)
  expect_equal(obj$worker_config_read("quiet"), list(verbose = FALSE))
})
