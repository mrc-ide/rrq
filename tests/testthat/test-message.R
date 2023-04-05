test_that("TIMEOUT_SET causes worker exit on idle worker", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  obj$message_send("TIMEOUT_SET", 0)
  w$step(TRUE)
  expect_equal(r6_private(w)$timeout_idle, 0)
  expect_is_function(r6_private(w)$timer)
  expect_lt(r6_private(w)$timer(), 0)
  expect_error(w$step(TRUE), "TIMEOUT", class = "rrq_worker_stop")
})


test_that("TIMEOUT_SET needs numeric input", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  id <- obj$message_send("TIMEOUT_SET", "soon")
  w$step(TRUE)
  expect_null(r6_private(w)$timeout_idle)
  expect_null(r6_private(w)$timer)
  expect_equal(
    obj$message_get_response(id, w$id),
    set_names(list("INVALID"), w$id))
})


test_that("TIMEOUT_SET with null clears a timer", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  obj$message_send("TIMEOUT_SET", 1)
  w$step(TRUE)
  expect_equal(r6_private(w)$timeout_idle, 1)
  expect_is_function(r6_private(w)$timer)
  obj$message_send("TIMEOUT_SET", NULL)
  w$step(TRUE)
  expect_null(r6_private(w)$timeout_idle)
  expect_null(r6_private(w)$timer)
})


test_that("TIMEOUT_GET returns infinite time if no timeout set", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- obj$message_send("TIMEOUT_GET")
  w$step(TRUE)
  expect_equal(
    obj$message_get_response(id, w$id),
    set_names(list(c(timeout_idle = Inf, remaining = Inf)), w$id))
})


## NOTE: small pauses for windows to deal with the time resolution
test_that("TIMEOUT_GET returns time remaining", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  obj$message_send("TIMEOUT_SET", 100)
  w$step(TRUE)
  expect_is_function(r6_private(w)$timer)

  Sys.sleep(0.1)
  id <- obj$message_send("TIMEOUT_GET")
  w$step(TRUE)

  response <- obj$message_get_response(id, w$id)
  expect_type(response, "list")
  expect_equal(names(response), w$id)
  expect_equal(response[[1]][["timeout_idle"]], 100)
  expect_lt(response[[1]][["remaining"]], 100)
  Sys.sleep(0.1)
  expect_gt(response[[1]][["remaining"]], r6_private(w)$timer())
})


test_that("TIMEOUT_GET restores a timer", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  obj$message_send("TIMEOUT_SET", 100)
  w$step(TRUE)
  obj$enqueue(sin(1))
  w$step(TRUE)
  expect_equal(r6_private(w)$timeout_idle, 100)
  expect_null(r6_private(w)$timer)

  id <- obj$message_send("TIMEOUT_GET")
  w$step(TRUE)
  res <- obj$message_get_response(id, w$id)[[1]]
  expect_equal(res[["timeout_idle"]], 100)
  expect_equal(res[["remaining"]], 100)
  expect_null(r6_private(w)$timer)

  w$step(TRUE)
  expect_is_function(r6_private(w)$timer)
})


test_that("message response getting", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  id <- obj$message_send("PING")
  expect_type(id, "character")
  expect_s3_class(redux::redis_time_to_r(id), "POSIXct")

  ## Not yet a response:
  expect_equal(obj$message_has_response(id, w$id), set_names(FALSE, w$id))
  expect_equal(obj$message_has_response(id), set_names(FALSE, w$id))
  expect_equal(obj$message_has_response(id, w$id, named = FALSE), FALSE)
  expect_equal(obj$message_has_response(id, named = FALSE), FALSE)

  ## Move worker through one cycle
  expect_message(w$step(TRUE), "PONG")

  expect_equal(obj$message_has_response(id, w$id), set_names(TRUE, w$id))
  expect_equal(obj$message_response_ids(w$id), id)

  ## Getting a response does not delete it by default
  expect_equal(obj$message_get_response(id), set_names(list("PONG"), w$id))
  expect_true(obj$message_has_response(id, w$id))

  ## But once deleted it is gone
  expect_equal(obj$message_get_response(id, delete = TRUE),
               set_names(list("PONG"), w$id))
  expect_false(obj$message_has_response(id, w$id))
})


test_that("Error on missing response", {
  obj <- test_rrq()
  w1 <- test_worker_blocking(obj)
  w2 <- test_worker_blocking(obj)
  nms <- c(w1$id, w2$id)

  id <- obj$message_send("PING")
  expect_equal(
    obj$message_has_response(id, nms),
    set_names(c(FALSE, FALSE), nms))
  expect_error(obj$message_get_response(id, nms, timeout = 0),
    sprintf("Response missing for workers: %s, %s", w1$id, w2$id))

  ## Also sensible if we do poll:
  expect_error(
    obj$message_get_response(id, nms, timeout = 0.1, time_poll = 0.1),
    sprintf("Response missing for workers: %s, %s", w1$id, w2$id))

  expect_message(w1$step(TRUE), "PONG")

  expect_equal(
    obj$message_has_response(id, nms),
    set_names(c(TRUE, FALSE), nms))
  expect_error(
    obj$message_get_response(id, nms, timeout = 0),
    sprintf("Response missing for workers: %s", w2$id))
})


test_that("ECHO", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- obj$message_send("ECHO", "hello world")
  expect_message(w$step(TRUE), "hello world")

  expect_equal(
    obj$message_get_response(id, timeout = 1),
    set_names(list("OK"), w$id))
})


test_that("EVAL", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id1 <- obj$message_send("EVAL", "1 + 1")
  id2 <- obj$message_send("EVAL", quote(2 + 2))
  expect_output(w$step(TRUE), "2\\s*$")
  expect_equal(obj$message_get_response(id1, w$id, timeout = 1),
               set_names(list(2), w$id))

  expect_output(w$step(TRUE), "4\\s*$")
  expect_equal(obj$message_get_response(id2, w$id, timeout = 1),
               set_names(list(4), w$id))
})


test_that("INFO", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- obj$message_send("INFO")
  w$step()

  res <- obj$message_get_response(id, w$id, timeout = 1)[[1]]
  expect_identical(res, w$info())
  expect_equal(res$worker, w$id)
  expect_equal(res$hostname, hostname())
})


test_that("STOP", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- obj$message_send("STOP")
  expect_error(w$step(TRUE), "BYE", class = "rrq_worker_stop")
})


test_that("messages take priority over tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id_task <- obj$enqueue(sin(1))
  id_message <- obj$message_send("PING")

  expect_message(w$step(TRUE), "PONG")
  expect_true(obj$message_has_response(id_message, w$id))
  expect_equal(obj$task_status(id_task), set_names(TASK_PENDING, id_task))

  w$step(TRUE)

  expect_equal(obj$task_status(id_task), set_names(TASK_COMPLETE, id_task))
})


test_that("PAUSE: workers ignore tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- obj$message_send("PAUSE")
  w$step(TRUE)

  expect_equal(obj$worker_status(), set_names(WORKER_PAUSED, w$id))

  task <- obj$enqueue(sin(1))
  expect_silent(w$step(TRUE))

  expect_equal(obj$task_status(task), set_names(TASK_PENDING, task))

  id <- obj$message_send("RESUME")
  w$step(TRUE)
  expect_equal(obj$worker_status(), set_names(WORKER_IDLE, w$id))

  expect_equal(obj$task_status(task), set_names(TASK_PENDING, task))

  w$step(TRUE)
  expect_equal(obj$task_status(task), set_names(TASK_COMPLETE, task))
})


test_that("PAUSE: workers accept messages", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- obj$message_send("PAUSE")
  w$step(TRUE)

  id <- obj$message_send("PING")
  expect_message(w$step(TRUE), "PONG")

  id <- obj$message_send("STOP")
  expect_error(w$step(TRUE), "BYE", class = "rrq_worker_stop")
})


test_that("PAUSE/RESUME: twice is noop", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id1 <- obj$message_send("RESUME")
  w$step(TRUE)

  expect_equal(obj$message_get_response(id1, w$id),
               set_names(list("NOOP"), w$id))

  id2 <- obj$message_send("PAUSE")
  id3 <- obj$message_send("PAUSE")
  w$step(TRUE)
  w$step(TRUE)

  expect_equal(obj$message_get_response(id3, w$id),
               set_names(list("NOOP"), w$id))
})


test_that("REFRESH", {
  root <- tempfile()
  obj <- test_rrq("myfuns.R", root)
  myfuns <- file.path(root, "myfuns.R")

  w <- test_worker_blocking(obj)

  t1 <- obj$enqueue(f1(1))
  w$step(TRUE)
  expect_equal(obj$task_result(t1), 2)

  writeLines("f1 <- function(x) x + 2", myfuns)
  t2 <- obj$enqueue(f1(2))
  w$step(TRUE)
  expect_equal(obj$task_result(t2), 3)

  id <- obj$message_send("REFRESH")
  w$step(TRUE)
  expect_equal(obj$message_get_response(id, w$id, FALSE), list("OK"))

  t3 <- obj$enqueue(f1(3))
  w$step(TRUE)
  expect_equal(obj$task_result(t3), 5)
})


test_that("unknown command", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- obj$message_send("XXXX")
  expect_message(
    w$step(TRUE),
    "Recieved unknown message: [XXXX]", fixed = TRUE)

  res <- obj$message_get_response(id, w$id, timeout = 1)[[1]]
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "XXXX")
  expect_null(res$args)
})


test_that("unknown command with arguments", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- obj$message_send("XXXX", "YYYY")
  expect_message(
    w$step(TRUE),
    "Recieved unknown message: [XXXX]", fixed = TRUE)

  res <- obj$message_get_response(id, w$id, timeout = 1)[[1]]
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "XXXX")
  expect_equal(res$args, "YYYY")
})


test_that("unknown command with complex arguments", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- obj$message_send("XXXX", mtcars)
  expect_message(
    w$step(TRUE),
    "Recieved unknown message: [XXXX]", fixed = TRUE)

  res <- obj$message_get_response(id, w$id, timeout = 1)[[1]]
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "XXXX")
  expect_equal(res$args, mtcars)
})


test_that("send and wait", {
  obj <- test_rrq()

  obj$worker_config_save("localhost", time_poll = 1)
  wid <- test_worker_spawn(obj, 5)

  st <- obj$worker_status()
  expect_equal(sort(names(st)), sort(wid))
  expect_equal(unname(st), rep(WORKER_IDLE, length(wid)))

  res <- obj$message_send_and_wait("PING")
  expect_equal(sort(names(res)), sort(wid))
  expect_equal(unname(res), rep(list("PONG"), length(wid)))

  ## Send to just one worker:
  res <- obj$message_send_and_wait("PING", worker_ids = wid[[1]])
  expect_equal(res, set_names(list("PONG"), wid[[1]]))

  ## Don't delete:
  res <- obj$message_send_and_wait("PING", worker_ids = wid[[1]],
                                   delete = FALSE)

  expect_equal(res[[1]], "PONG")
  expect_equal(names(res), wid[[1]])
  id <- attr(res, "message_id")
  expect_type(id, "character")
  expect_equal(obj$message_get_response(id, wid[[1]]),
               set_names(list("PONG"), wid[[1]]))

  res <- obj$message_send_and_wait("STOP")
  expect_equal(sort(names(res)), sort(wid))
  expect_equal(unname(res), rep(list("BYE"), length(wid)))
})


test_that("Some messages reset timer", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  obj$message_send("TIMEOUT_SET", 100)
  w$step(TRUE)
  expect_is_function(r6_private(w)$timer)

  id <- obj$message_send("TIMEOUT_GET")
  w$step(TRUE)
  expect_is_function(r6_private(w)$timer)

  f <- function(cmd, args = NULL) {
    obj$message_send("TIMEOUT_SET", 100)
    w$step(TRUE)
    stopifnot(is.function(r6_private(w)$timer))
    id <- obj$message_send(cmd, args)
    w$step(TRUE)
    expect_null(r6_private(w)$timer)
  }

  suppressMessages(f("PING"))
  suppressMessages(f("ECHO", "hello"))
  capture.output(f("EVAL", "1 + 1"))
  f("INFO")
  f("PAUSE")
  f("RESUME")
  f("REFRESH")
})
