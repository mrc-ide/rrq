test_that("rrq_default configuration", {
  ## (this is actually the *test* default configuration, which is
  ## possibly not what is wanted)
  obj <- test_rrq()
  expect_equal(rrq_worker_config_list(controller = obj), WORKER_CONFIG_DEFAULT)
  expect_equal(rrq_worker_config_read(WORKER_CONFIG_DEFAULT, controller = obj),
               rrq_worker_config(poll_queue = 1, verbose = FALSE))
})


test_that("create short-lived worker", {
  obj <- test_rrq()

  key <- "stop_immediately"
  cfg <- rrq_worker_config(timeout_idle = 0, poll_queue = 1, verbose = TRUE)
  rrq_worker_config_save2(key, cfg, controller = obj)

  ## Local:
  msg1 <- capture_messages(
    w <- rrq_worker$new(obj$queue_id, key))
  msg2 <- capture_messages(res <- w$loop())
  expect_null(res)
  expect_true(any(grepl("STOP OK (TIMEOUT)", msg2, fixed = TRUE)))

  ## Remote:
  logs <- tempfile()
  w <- test_worker_spawn(obj, name_config = key, logdir = logs)
  expect_type(w$id, "character")
  log <- rrq_worker_log_tail(w$id, Inf, controller = obj)
  expect_s3_class(log, "data.frame")
  expect_true(nrow(log) >= 1)

  remaining <- time_checker(3)
  while (remaining() > 0) {
    log <- rrq_worker_log_tail(w$id, Inf, controller = obj)
    if (nrow(log) >= 5L) {
      break
    } else {
      Sys.sleep(0.1)
    }
  }
  expect_equal(nrow(log), 5L)
  expect_equal(log$command[[5]], "STOP")

  expect_true(any(grepl("STOP OK (TIMEOUT)", w$logs(1), fixed = TRUE)))
})


test_that("Sensible error message on missing config", {
  obj <- test_rrq()

  key <- "nonexistant"

  msg <- capture_messages(expect_error(
    rrq_worker$new(obj$queue_id, key),
    "Invalid rrq worker configuration key 'nonexistant'"))
  expect_error(
    test_worker_spawn(obj, name_config = key),
    "Invalid rrq worker configuration key 'nonexistant'")
})


test_that("worker timeout", {
  obj <- test_rrq("myfuns.R")

  t <- as.integer(runif(1, min = 100, max = 10000))
  cfg <- rrq_worker_config(timeout_idle = t, verbose = FALSE)
  res <- rrq_worker_config_save2(WORKER_CONFIG_DEFAULT, cfg, controller = obj)
  expect_true(res)
  expect_equal(cfg$timeout_idle, t)

  w <- test_worker_blocking(obj)
  expect_equal(r6_private(w)$timeout_idle, t)
  expect_lte(r6_private(w)$timer(), t)

  id <- obj$message_send("TIMEOUT_GET")
  w$step(TRUE)
  res <- obj$message_get_response(id, w$id)[[1]]
  expect_equal(res[["timeout_idle"]], t)
  expect_lte(res[["remaining"]], t)
})


test_that("infinite timeout", {
  obj <- test_rrq("myfuns.R")
  cfg <- rrq_worker_config(timeout_idle = Inf, verbose = FALSE)
  rrq_worker_config_save2("infinite", cfg, controller = obj)

  w <- test_worker_blocking(obj, name_config = "infinite")
  expect_equal(r6_private(w)$timeout_idle, Inf)
  expect_null(r6_private(w)$timer)

  id <- obj$message_send("TIMEOUT_GET")
  w$step(TRUE)
  res <- obj$message_get_response(id, w$id)[[1]]
  expect_equal(res, c(timeout_idle = Inf, remaining = Inf))
})



test_that("rrq_default configuration", {
  ## (this is actually the *test* default configuration, which is
  ## possibly not what is wanted)
  obj <- test_rrq()
  cfg1 <- rrq_worker_config(timeout_idle = 1)
  cfg2 <- rrq_worker_config(timeout_idle = 2)
  expect_true(rrq_worker_config_save2("new", cfg1, overwrite = FALSE, controller = obj))
  expect_equal(rrq_worker_config_read("new", controller = obj), cfg1)
  expect_false(rrq_worker_config_save2("new", cfg2, overwrite = FALSE, controller = obj))
  expect_equal(rrq_worker_config_read("new", controller = obj), cfg1)
})


test_that("verbose is validated", {
  expect_error(
    rrq_worker_config(verbose = "no thank you"),
    "'verbose' must be a logical")
  expect_false(rrq_worker_config(verbose = FALSE)$verbose)
})

test_that("poll_process is validated", {
  expect_error(
    rrq_worker_config(poll_process = "5"),
    "'poll_process' must be a numeric")
  expect_equal(rrq_worker_config(poll_process = 5)$poll_process, 5)
})

test_that("timeout_process_die is validated", {
  expect_error(
    rrq_worker_config(timeout_process_die = "5"),
    "'timeout_process_die' must be a numeric")
  expect_equal(rrq_worker_config(timeout_process_die = 5)$timeout_process_die,
               5)
})


test_that("can save worker configuration with top-level function", {
  obj <- test_rrq()
  cfg <- rrq_worker_config(timeout_idle = 10, verbose = FALSE)
  rrq_worker_config_save2(WORKER_CONFIG_DEFAULT, cfg, controller = obj)
  obj2 <- rrq_controller$new(obj$queue_id)
  expect_equal(
    rrq_worker_config_read(WORKER_CONFIG_DEFAULT, controller = obj2),
    cfg)
})


test_that("default worker config poll queue depends on interactivity", {
  expect_equal(
    rlang::with_interactive(rrq_worker_config()$poll_queue, TRUE),
    5)
  expect_equal(
    rlang::with_interactive(rrq_worker_config()$poll_queue, FALSE),
    60)
  expect_equal(
    rlang::with_interactive(
      rrq_worker_config(poll_queue = 20)$poll_queue, TRUE),
    20)
  expect_equal(
    rlang::with_interactive(
      rrq_worker_config(poll_queue = 20)$poll_queue, FALSE),
    20)
})


test_that("can timeout while reading a configuration", {
  skip_if_not_installed("mockery")
  obj <- test_rrq()
  cfg <- rrq_worker_config_read(WORKER_CONFIG_DEFAULT, controller = obj)
  mock_wait <- mockery::mock(cfg)
  mockery::stub(rrq_worker_config_read, "wait_success", mock_wait)
  res <- rrq_worker_config_read(WORKER_CONFIG_DEFAULT, 30, obj)
  expect_equal(res, cfg)
  mockery::expect_called(mock_wait, 1)
  args <- mockery::mock_args(mock_wait)[[1]]
  expect_equal(args[[2]], 30)
  expect_equal(args[[4]], 1)
  expect_equal(args[[3]](), cfg)
})
