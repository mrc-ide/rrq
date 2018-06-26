context("messaging")

test_that("basic", {
  Sys.setenv(R_TESTS = "")
  root <- tempfile()
  context <- context::context_save(root, sources = "myfuns.R")
  context <- context::context_load(context, new.env(parent = .GlobalEnv))
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  obj$worker_config_save("localhost", time_poll = 1, copy_redis = TRUE)
  wid <- worker_spawn(obj, timeout = 5, progress = PROGRESS)

  ## First, let's test the basic messaging approach:
  ##
  ## TODO: This needs testing with >1 worker.
  id <- obj$message_send("PING")
  expect_is(id, "character")
  expect_is(redux::redis_time_to_r(id), "POSIXct")

  Sys.sleep(0.1)
  expect_equal(obj$message_has_response(id, wid), setNames(TRUE, wid))
  expect_equal(obj$message_has_response(id), setNames(TRUE, wid))
  expect_equal(obj$message_has_response(id, wid, named = FALSE), TRUE)

  expect_equal(obj$message_get_response(id, wid), setNames(list("PONG"), wid))
  expect_equal(obj$message_get_response(id), setNames(list("PONG"), wid))
  expect_equal(obj$message_get_response(id, wid, named = FALSE), list("PONG"))

  expect_equal(obj$message_response_ids(wid), id)

  expect_equal(obj$message_get_response(id, timeout = 10),
               setNames(list("PONG"), wid))

  expect_equal(obj$message_get_response(id, delete = TRUE),
               setNames(list("PONG"), wid))
  expect_equal(obj$message_response_ids(wid), character(0))
  expect_equal(obj$message_has_response(id, wid),
               setNames(FALSE, wid))
  expect_error(obj$message_get_response(id, wid), "Response missing")
  expect_error(obj$message_get_response(id), "Response missing")

  ## Next, echo:
  ## TODO: should this not echo it back as the response?
  id <- obj$message_send("ECHO", "hello world")
  expect_equal(obj$message_get_response(id, timeout = 1),
               setNames(list("OK"), wid))

  ## Eval
  id <- obj$message_send("EVAL", "1 + 1")
  expect_equal(obj$message_get_response(id, wid, timeout = 1, named = FALSE),
               list(2))
  expect_equal(obj$message_get_response(id, wid, timeout = 1),
               setNames(list(2), wid))

  id <- obj$message_send("EVAL", quote(1 + 2))
  expect_equal(obj$message_get_response(id, wid, timeout = 1),
               setNames(list(3), wid))

  id <- obj$message_send("INFO")
  res <- obj$message_get_response(id, wid, timeout = 1)[[1]]
  expect_is(res, "worker_info")
  expect_equal(res$worker, wid)
  expect_equal(res$hostname, hostname())

  id <- obj$message_send("STOP")
  res <- obj$message_get_response(id, wid, timeout = 1)
  expect_equal(res, setNames(list("BYE"), wid))
  expect_equal(obj$worker_list(), character(0))
  expect_equal(obj$worker_list_exited(), wid)
})

test_that("pause", {
  Sys.setenv(R_TESTS = "")
  root <- tempfile()
  context <- context::context_save(root, sources = "myfuns.R")
  context <- context::context_load(context, new.env(parent = .GlobalEnv))
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  obj$worker_config_save("localhost", time_poll = 1, copy_redis = TRUE)
  wid <- worker_spawn(obj, timeout = 5, progress = PROGRESS)

  expect_equal(obj$worker_status(),
               setNames(WORKER_IDLE, wid))

  id <- obj$message_send("PAUSE")
  expect_equal(obj$message_get_response(id, wid, timeout = 1),
               setNames(list("OK"), wid))

  expect_equal(obj$worker_status(),
               setNames(WORKER_PAUSED, wid))

  ## Can still ping the worker quite happily:
  id <- obj$message_send("PING")
  expect_equal(obj$message_get_response(id, wid, timeout = 1),
               setNames(list("PONG"), wid))

  ## Pausing again is a noop:
  id <- obj$message_send("PAUSE")
  expect_equal(obj$message_get_response(id, wid, timeout = 1),
               setNames(list("NOOP"), wid))

  t <- obj$enqueue(sin(1))
  ## should have been queued by now if the worker was interested:
  Sys.sleep(.5)
  expect_equal(obj$task_status(t), setNames(TASK_PENDING, t))

  id <- obj$message_send("RESUME")
  expect_equal(obj$message_get_response(id, wid, timeout = 1),
               setNames(list("OK"), wid))

  id <- obj$message_send("RESUME")
  expect_equal(obj$message_get_response(id, wid, timeout = 1),
               setNames(list("NOOP"), wid))

  res <- obj$task_wait(t, 1)
  expect_equal(res, sin(1))

  expect_equal(obj$worker_status(),
               setNames(WORKER_IDLE, wid))

  ## Check the log.
  log <- obj$worker_log_tail(wid, Inf)

  cmp_cmd <- c("ALIVE",
               rep(c("MESSAGE", "RESPONSE"), 4),
               "TASK_START", "TASK_COMPLETE", "MESSAGE", "RESPONSE")
  cmp_msg <- c("",
               rep(c("PAUSE", "PING", "PAUSE", "RESUME"), each = 2),
               t, t, "RESUME", "RESUME")
  expect_equal(log$command, cmp_cmd)
  expect_equal(log$message, cmp_msg)

  ## Will stop when paused:
  id <- obj$message_send("PAUSE")
  expect_equal(obj$message_get_response(id, wid, timeout = 1),
               setNames(list("OK"), wid))
  id <- obj$message_send("STOP")
  expect_equal(obj$message_get_response(id, wid, timeout = 1),
               setNames(list("BYE"), wid))

  expect_equal(obj$worker_list_exited(), wid)
})

test_that("unknown command", {
  Sys.setenv(R_TESTS = "")
  root <- tempfile()
  context <- context::context_save(root, sources = "myfuns.R")
  context <- context::context_load(context, new.env(parent = .GlobalEnv))
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  obj$worker_config_save("localhost", time_poll = 1, copy_redis = TRUE)
  wid <- worker_spawn(obj, timeout = 5, progress = PROGRESS)

  expect_equal(obj$worker_status(),
               setNames(WORKER_IDLE, wid))

  id <- obj$message_send("XXXX")
  res <- obj$message_get_response(id, wid, timeout = 1)[[1]]
  expect_is(res, "condition")
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "XXXX")
  expect_null(res$args)

  id <- obj$message_send("YYYY", "ZZZZ")
  res <- obj$message_get_response(id, wid, timeout = 1)[[1]]
  expect_is(res, "condition")
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "YYYY")
  expect_equal(res$args, "ZZZZ")

  ## Complex arguments are supported:
  d <- data.frame(a = 1, b = 2)
  id <- obj$message_send("YYYY", d)
  res <- obj$message_get_response(id, wid, timeout = 1)[[1]]
  expect_is(res, "condition")
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "YYYY")
  expect_equal(res$args, d)
})

test_that("send and wait", {
  Sys.setenv(R_TESTS = "")
  root <- tempfile()
  context <- context::context_save(root, sources = "myfuns.R")
  context <- context::context_load(context, new.env(parent = .GlobalEnv))
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  obj$worker_config_save("localhost", time_poll = 1, copy_redis = TRUE)
  wid <- worker_spawn(obj, 5, timeout = 5, progress = PROGRESS)

  st <- obj$worker_status()
  expect_equal(sort(names(st)), sort(wid))
  expect_equal(unname(st), rep(WORKER_IDLE, length(wid)))

  res <- obj$message_send_and_wait("PING")
  expect_equal(sort(names(res)), sort(wid))
  expect_equal(unname(res), rep(list("PONG"), length(wid)))

  ## Send to just one worker:
  res <- obj$message_send_and_wait("PING", worker_ids = wid[[1]])
  expect_equal(res, setNames(list("PONG"), wid[[1]]))

  ## Don't delete:
  res <- obj$message_send_and_wait("PING", worker_ids = wid[[1]],
                                   delete = FALSE)
  expect_equal(res[[1]], "PONG")
  expect_equal(names(res), wid[[1]])
  id <- attr(res, "message_id")
  expect_is(id, "character")
  expect_equal(obj$message_get_response(id, wid[[1]]),
               setNames(list("PONG"), wid[[1]]))

  res <- obj$message_send_and_wait("STOP")
  expect_equal(sort(names(res)), sort(wid))
  expect_equal(unname(res), rep(list("BYE"), length(wid)))
})

test_that("refresh", {
  Sys.setenv(R_TESTS = "")
  root <- tempfile()
  myfuns <- tempfile("myfuns_", ".", ".R")
  writeLines("f <- function(x) x * 2", myfuns)

  context <- context::context_save(root, sources = myfuns)
  context <- context::context_load(context, new.env(parent = .GlobalEnv))
  obj <- rrq_controller(context, redux::hiredis())
  on.exit({
    obj$destroy()
    file.remove(myfuns)
  })

  obj$worker_config_save("localhost", time_poll = 1, copy_redis = TRUE)
  wid <- worker_spawn(obj, 1, timeout = 5, progress = PROGRESS)

  t1 <- obj$enqueue(f(10))
  expect_equal(obj$task_wait(t1, 10, progress = FALSE), 20)

  writeLines("f <- function(x) x * 3", myfuns)
  t2 <- obj$enqueue(f(20))
  expect_equal(obj$task_wait(t2, 10, progress = FALSE), 40)

  res <- obj$message_send_and_wait("REFRESH")
  expect_equal(res, setNames(list("OK"), wid))

  t3 <- obj$enqueue(f(30))
  expect_equal(obj$task_wait(t3, 10, progress = FALSE), 90)
})

test_that("timeout", {
  Sys.setenv(R_TESTS = "")
  root <- tempfile()
  context <- context::context_save(root, sources = "myfuns.R")
  context <- context::context_load(context, new.env(parent = .GlobalEnv))
  obj <- rrq_controller(context, redux::hiredis())
  on.exit(obj$destroy())

  obj$worker_config_save("localhost", time_poll = 1, copy_redis = TRUE)
  wid <- worker_spawn(obj, timeout = 5, progress = PROGRESS)

  expect_equal(obj$message_send_and_wait("TIMEOUT_GET"),
               setNames(list(c(timeout = Inf, remaining = Inf)), wid))
  expect_equal(obj$message_send_and_wait("TIMEOUT_SET", 1000),
               setNames(list("OK"), wid))
  res <- obj$message_send_and_wait("TIMEOUT_GET")
  expect_equal(names(res), wid)
  expect_equal(res[[1]][["timeout"]], 1000)
  expect_lte(res[[1]][["remaining"]], 1000)

  Sys.sleep(1)

  ## This just checks that the timeout is reset correctly after a task
  ## and that the timer deletion on task does not cause problems.
  t <- obj$enqueue(sin(1))
  res <- obj$message_send_and_wait("TIMEOUT_GET")[[1]]
  expect_equal(res[["timeout"]], 1000)
  expect_gt(res[["remaining"]], 999.5)

  ## Clear the timeout:
  expect_equal(obj$message_send_and_wait("TIMEOUT_SET", NULL),
               setNames(list("OK"), wid))
  expect_equal(obj$message_send_and_wait("TIMEOUT_GET"),
               setNames(list(c(timeout = Inf, remaining = Inf)), wid))
  expect_equal(obj$message_send_and_wait("TIMEOUT_SET", Inf),
               setNames(list("OK"), wid))
  expect_equal(obj$message_send_and_wait("TIMEOUT_GET"),
               setNames(list(c(timeout = Inf, remaining = Inf)), wid))

  expect_equal(obj$message_send_and_wait("TIMEOUT_SET", "hello!"),
               setNames(list("INVALID"), wid))
  expect_equal(obj$message_send_and_wait("TIMEOUT_GET"),
               setNames(list(c(timeout = Inf, remaining = Inf)), wid))

  expect_equal(obj$message_send_and_wait("TIMEOUT_SET", 0),
               setNames(list("OK"), wid))

  Sys.sleep(1.2)

  expect_equal(obj$worker_list(), character(0))
  expect_equal(obj$worker_list_exited(), wid)
})
