context("messaging")

test_that("timeout", {
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  wid <- workers_spawn(context, obj$con, logdir="logs", worker_time_poll=1)

  ## First, let's test the basic messaging approach:
  ##
  ## TODO: This needs testing with >1 worker.
  id <- obj$send_message("PING")
  expect_is(id, "character")
  expect_is(redux::redis_time_to_r(id), "POSIXct")

  Sys.sleep(0.1)
  expect_equal(obj$has_responses(id, wid), setNames(TRUE, wid))
  expect_equal(obj$has_responses(id), setNames(TRUE, wid))
  expect_equal(obj$has_response(id, wid), TRUE)

  expect_equal(obj$get_responses(id, wid), setNames(list("PONG"), wid))
  expect_equal(obj$get_responses(id), setNames(list("PONG"), wid))
  expect_equal(obj$get_response(id, wid), "PONG")

  expect_equal(obj$response_ids(wid), id)

  expect_equal(obj$get_responses(id, wait=10), setNames(list("PONG"), wid))

  expect_equal(obj$get_responses(id, delete=TRUE), setNames(list("PONG"), wid))
  expect_equal(obj$response_ids(wid), character(0))
  expect_equal(obj$has_response(id, wid), FALSE)
  expect_error(obj$get_response(id, wid), "Response missing")
  expect_error(obj$get_responses(id, wid), "Response missing")
  expect_error(obj$get_responses(id), "Response missing")

  ## Next, echo:
  id <- obj$send_message("ECHO", "hello world")
  expect_equal(obj$get_responses(id, wait=1), setNames(list("OK"), wid))

  ## Eval
  id <- obj$send_message("EVAL", "1 + 1")
  expect_equal(obj$get_response(id, wid, wait=1), 2)

  id <- obj$send_message("EVAL", quote(1 + 1))
  expect_equal(obj$get_response(id, wid, wait=1), 2)

  id <- obj$send_message("INFO")
  res <- obj$get_response(id, wid, wait=1)
  expect_is(res, "worker_info")
  expect_equal(res$worker, wid)
  expect_equal(res$hostname, hostname())

  id <- obj$send_message("TIMEOUT_GET")
  expect_equal(obj$get_response(id, wid, wait=1),
               c(timeout=Inf, remaining=Inf))

  id <- obj$send_message("TIMEOUT_SET", 1000)
  expect_equal(obj$get_response(id, wid, wait=1), "OK")

  id <- obj$send_message("TIMEOUT_GET")
  res <- obj$get_response(id, wid, wait=1)
  expect_equal(res[["timeout"]], 1000)
  expect_lte(res[["remaining"]], 1000)

  id <- obj$send_message("TIMEOUT_SET", 0)
  expect_equal(obj$get_response(id, wid, wait=1), "OK")

  Sys.sleep(1)

  expect_equal(obj$workers_list(), character(0))
  expect_equal(obj$workers_list_exited(), wid)
})

test_that("pause", {
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  wid <- workers_spawn(context, obj$con, logdir="logs", worker_time_poll=1)

  expect_that(obj$workers_status(),
              equals(setNames(WORKER_IDLE, wid)))

  id <- obj$send_message("PAUSE")
  expect_that(obj$get_response(id, wid, wait=1), equals("OK"))

  expect_that(obj$workers_status(),
              equals(setNames(WORKER_PAUSED, wid)))

  ## Can still ping the worker quite happily:
  id <- obj$send_message("PING")
  expect_that(obj$get_response(id, wid, wait=1), equals("PONG"))

  ## Pausing again is a noop:
  id <- obj$send_message("PAUSE")
  expect_that(obj$get_response(id, wid, wait=1), equals("NOOP"))

  t <- obj$enqueue(sin(1))
  ## should have been queued by now if the worker was interested:
  Sys.sleep(.5)
  expect_equal(obj$tasks_status(t), setNames(TASK_PENDING, t))
  expect_equal(obj$task_status(t), TASK_PENDING)

  id <- obj$send_message("RESUME")
  expect_that(obj$get_response(id, wid, wait=1), equals("OK"))

  res <- obj$task_wait(t, 1)
  expect_equal(res, sin(1))

  expect_that(obj$workers_status(),
              equals(setNames(WORKER_IDLE, wid)))

  ## Check the log.

  log <- obj$workers_log_tail(wid, 0)

  expect_that(log$command, equals(c("ALIVE",
                                    rep(c("MESSAGE", "RESPONSE"), 4),
                                    "TASK_START", "TASK_COMPLETE")))
  expect_that(log$message, equals(c("",
                                    rep(c("PAUSE", "PING", "PAUSE", "RESUME"),
                                        each=2),
                                    t, t)))

  ## Will stop when paused:
  id <- obj$send_message("PAUSE")
  expect_that(obj$get_response(id, wid, wait=1), equals("OK"))
  id <- obj$send_message("STOP")
  expect_that(obj$get_response(id, wid, wait=1), equals("BYE"))

  expect_that(obj$workers_list_exited(), equals(wid))
})

test_that("unknown command", {
  Sys.setenv(R_TESTS="")
  root <- tempfile()
  context <- context::context_save(root, sources="myfuns.R")
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  wid <- workers_spawn(context, obj$con, logdir="logs", worker_time_poll=1)

  expect_that(obj$workers_status(),
              equals(setNames(WORKER_IDLE, wid)))

  id <- obj$send_message("XXXX")
  res <- obj$get_response(id, wid, wait=1)
  expect_that(res, is_a("condition"))
  expect_that(res$message, matches("Recieved unknown message"))
  expect_that(res$command, equals("XXXX"))
  expect_that(res$args, equals(NULL))

  id <- obj$send_message("YYYY", "ZZZZ")
  res <- obj$get_response(id, wid, wait=1)
  expect_that(res, is_a("condition"))
  expect_that(res$message, matches("Recieved unknown message"))
  expect_that(res$command, equals("YYYY"))
  expect_that(res$args, equals("ZZZZ"))

  ## Complex arguments are supported:
  d <- data.frame(a=1, b=2)
  id <- obj$send_message("YYYY", d)
  res <- obj$get_response(id, wid, wait=1)
  expect_that(res, is_a("condition"))
  expect_that(res$message, matches("Recieved unknown message"))
  expect_that(res$command, equals("YYYY"))
  expect_that(res$args, equals(d))
})
