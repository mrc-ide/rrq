context("worker_config")

test_that("rrq_default configuration", {
  path <- tempfile()
  on.exit(unlink(path, recursive = TRUE))

  expect_error(rrq_worker_main_args(c()), "At least 4 arguments required")
  ctx <- context::context_save(path)
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  obj <- rrq_controller(ctx, redux::hiredis())
  expect_equal(obj$db$get("localhost", "worker_config"),
               list(redis_host = obj$con$config()$host,
                    redis_port = obj$con$config()$port))
})

test_that("rrq_worker_main_args", {
  path <- tempfile()
  expect_error(rrq_worker_main_args(c()), "At least 4 arguments required")

  ctx <- context::context_save(path)
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  obj <- rrq_controller(ctx, redux::hiredis())

  host <- "redis_host"
  port <- 8888
  nm <- ids::adjective_animal()
  key <- ids::random_id()
  use <- "myconfig"
  args <- c(path, ctx$id, use, nm)
  args2 <- c(args, key)

  expect_error(rrq_worker_main_args(args),
               "Invalid rrq worker configuration key")
  expect_error(rrq_worker_main_args(args2),
               "Invalid rrq worker configuration key")

  ## Then save a configuration:
  obj$worker_config_save(use, redis_host = host, redis_port = port)
  config <- rrq_worker_main_args(args)

  ## And show that we can load it appropriately
  expect_equal(config$redis_host, host)
  expect_equal(config$redis_port, port)
  expect_equal(config$context_root, path)
  expect_equal(config$context_id, ctx$id)
  expect_equal(config$worker_name, nm)
  expect_equal(length(config), 5L) # as above

  config2 <- rrq_worker_main_args(args2)
  expect_equal(config2$key_alive, key)
  expect_equal(config2[names(config)], config)
  expect_equal(length(config2), 6L)
})

test_that("create short-lived worker", {
  path <- tempfile()
  ctx <- context::context_save(path)
  ctx <- context::context_load(ctx, new.env(parent = .GlobalEnv))
  obj <- rrq_controller(ctx, redux::hiredis())

  key <- "stop_immediately"
  cfg <- obj$worker_config_save(key, timeout = 0, time_poll = 1,
                                copy_redis = TRUE)

  ## Local:
  msg <- capture_messages(w <- rrq_worker_from_config(path, ctx$id, key))
  expect_null(w)
  expect_true(any(grepl("STOP OK (TIMEOUT)", msg, fixed = TRUE)))

  ## Remote:
  wid <- workers_spawn(obj, timeout = 10, worker_config = key, progress = FALSE)
  expect_is(wid, "character")
  log <- obj$workers_log_tail(wid, Inf)
  expect_is(log, "data.frame")
  expect_true(nrow(log) > 0)
  if (nrow(log) == 1L) {
    Sys.sleep(1.2)
  }
  log <- obj$workers_log_tail(wid, Inf)
  expect_equal(nrow(log), 2L)
  expect_equal(log$command[[2]], "STOP")
  expect_true(file.exists(file.path(path, "worker_logs", wid)))
  txt <- obj$worker_process_log(wid)
  expect_is(txt, "character")
  expect_true(any(grepl("STOP OK (TIMEOUT)", txt, fixed = TRUE)))
})
