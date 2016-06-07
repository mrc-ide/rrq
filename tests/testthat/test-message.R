context("messaging")

## So, on Tuesday:
##
## Port over messaging tests from rrqueue.  The system is basically
## the same so the test suite should work about the same, which is
## nice.
##
## Make sure my worker list functions filter off closed workers.  I
## see uncaught exceptions leave workers in the pool, but these could
## have been savage killing.
##
## Add an option to the worker launch script and set a default timeout
## of a few hours or something.  We can make it configurable later.
## This will stop the cluster filling up with endless workers.
##
## Look at the worker log thing and see if that can be checked easily.
##
## Think about re-queuing.  To get this in there consider ripping
## signalling out of redisheartbeat for now so we can get that up and
## running.  There's not a great need to do this though unless I can
## get automatic re-queuing working, and that's pretty challenging.
##
## Get and parse worker logs.  At the same time allow computing
## average worker load.
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
