test_that("TIMEOUT_SET causes worker exit on idle worker", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  rrq_message_send("TIMEOUT_SET", 0, controller = obj)
  w$step(TRUE)
  expect_equal(r6_private(w)$timeout_idle, 0)
  expect_is_function(r6_private(w)$timer)
  expect_lt(r6_private(w)$timer(), 0)
  expect_error(w$step(TRUE), "TIMEOUT", class = "rrq_worker_stop")
})


test_that("TIMEOUT_SET needs numeric input", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  id <- rrq_message_send("TIMEOUT_SET", "soon", controller = obj)
  w$step(TRUE)
  expect_equal(r6_private(w)$timeout_idle, Inf)
  expect_null(r6_private(w)$timer)
  expect_equal(
    rrq_message_get_response(id, w$id, controller = obj),
    set_names(list("INVALID"), w$id))
})


test_that("TIMEOUT_SET with null clears a timer", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  rrq_message_send("TIMEOUT_SET", 1, controller = obj)
  w$step(TRUE)
  expect_equal(r6_private(w)$timeout_idle, 1)
  expect_is_function(r6_private(w)$timer)
  rrq_message_send("TIMEOUT_SET", NULL, controller = obj)
  w$step(TRUE)
  expect_equal(r6_private(w)$timeout_idle, Inf)
  expect_null(r6_private(w)$timer)
})


test_that("TIMEOUT_GET returns infinite time if no timeout set", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- rrq_message_send("TIMEOUT_GET", controller = obj)
  w$step(TRUE)
  expect_equal(
    rrq_message_get_response(id, w$id, controller = obj),
    set_names(list(c(timeout_idle = Inf, remaining = Inf)), w$id))
})


## NOTE: small pauses for windows to deal with the time resolution
test_that("TIMEOUT_GET returns time remaining", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  rrq_message_send("TIMEOUT_SET", 100, controller = obj)
  w$step(TRUE)
  expect_is_function(r6_private(w)$timer)

  Sys.sleep(0.1)
  id <- rrq_message_send("TIMEOUT_GET", controller = obj)
  w$step(TRUE)

  response <- rrq_message_get_response(id, w$id, controller = obj)
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

  rrq_message_send("TIMEOUT_SET", 100, controller = obj)
  w$step(TRUE)
  rrq_task_create_expr(sin(1), controller = obj)
  w$step(TRUE)
  expect_equal(r6_private(w)$timeout_idle, 100)
  expect_null(r6_private(w)$timer)

  id <- rrq_message_send("TIMEOUT_GET", controller = obj)
  w$step(TRUE)
  res <- rrq_message_get_response(id, w$id, controller = obj)[[1]]
  expect_equal(res[["timeout_idle"]], 100)
  expect_equal(res[["remaining"]], 100)
  expect_null(r6_private(w)$timer)

  w$step(TRUE)
  expect_is_function(r6_private(w)$timer)
})


test_that("message response getting", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)
  id <- rrq_message_send("PING", controller = obj)
  expect_type(id, "character")
  expect_s3_class(redux::redis_time_to_r(id), "POSIXct")

  ## Not yet a response:
  expect_equal(rrq_message_has_response(id, w$id, controller = obj),
               set_names(FALSE, w$id))
  expect_equal(rrq_message_has_response(id, controller = obj),
               set_names(FALSE, w$id))
  expect_equal(rrq_message_has_response(id, w$id, named = FALSE,
                                        controller = obj),
               FALSE)
  expect_equal(rrq_message_has_response(id, named = FALSE, controller = obj),
               FALSE)

  ## Move worker through one cycle
  expect_message(w$step(TRUE), "PONG")

  expect_equal(rrq_message_has_response(id, w$id, controller = obj),
               set_names(TRUE, w$id))
  expect_equal(rrq_message_response_ids(w$id, controller = obj), id)

  ## Getting a response does not delete it by default
  expect_equal(rrq_message_get_response(id, controller = obj),
               set_names(list("PONG"), w$id))
  expect_true(rrq_message_has_response(id, w$id, controller = obj))

  ## But once deleted it is gone
  expect_equal(rrq_message_get_response(id, delete = TRUE, controller = obj),
               set_names(list("PONG"), w$id))
  expect_false(rrq_message_has_response(id, w$id, controller = obj))
})


test_that("Error on missing response", {
  obj <- test_rrq()
  w1 <- test_worker_blocking(obj, worker_id = "a")
  w2 <- test_worker_blocking(obj, worker_id = "b")
  nms <- c(w1$id, w2$id)

  id <- rrq_message_send("PING", controller = obj)
  expect_equal(
    rrq_message_has_response(id, nms, controller = obj),
    set_names(c(FALSE, FALSE), nms))
  expect_error(rrq_message_get_response(id, nms, timeout = 0, controller = obj),
    "Response missing for workers: 'a' and 'b'")

  ## Also sensible if we do poll:
  expect_error(
    rrq_message_get_response(id, nms, timeout = 0.1, time_poll = 0.1,
                             controller = obj),
    "Response missing for workers: 'a' and 'b'")

  expect_message(w1$step(TRUE), "PONG")

  expect_equal(
    rrq_message_has_response(id, nms, controller = obj),
    set_names(c(TRUE, FALSE), nms))
  expect_error(
    rrq_message_get_response(id, nms, timeout = 0, controller = obj),
    "Response missing for worker: 'b'")
})


test_that("ECHO", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- rrq_message_send("ECHO", "hello world", controller = obj)
  expect_message(w$step(TRUE), "hello world")

  expect_equal(
    rrq_message_get_response(id, timeout = 1, controller = obj),
    set_names(list("OK"), w$id))
})


test_that("EVAL", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id1 <- rrq_message_send("EVAL", "1 + 1", controller = obj)
  id2 <- rrq_message_send("EVAL", quote(2 + 2), controller = obj)
  expect_output(w$step(TRUE), "2\\s*$")
  expect_equal(rrq_message_get_response(id1, w$id, timeout = 1,
                                        controller = obj),
               set_names(list(2), w$id))

  expect_output(w$step(TRUE), "4\\s*$")
  expect_equal(rrq_message_get_response(id2, w$id, timeout = 1,
                                        controller = obj),
               set_names(list(4), w$id))
})


test_that("INFO", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- rrq_message_send("INFO", controller = obj)
  w$step()

  res <- rrq_message_get_response(id, w$id, timeout = 1, controller = obj)[[1]]
  expect_identical(res, w$info())
  expect_equal(res$worker, w$id)
  expect_equal(res$hostname, hostname())
})


test_that("STOP", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- rrq_message_send("STOP", controller = obj)
  expect_error(w$step(TRUE), "BYE", class = "rrq_worker_stop")
})


test_that("messages take priority over tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id_task <- rrq_task_create_expr(sin(1), controller = obj)
  id_message <- rrq_message_send("PING", controller = obj)

  expect_message(w$step(TRUE), "PONG")
  expect_true(rrq_message_has_response(id_message, w$id, controller = obj))
  expect_equal(rrq_task_status(id_task, controller = obj), TASK_PENDING)

  w$step(TRUE)

  expect_equal(rrq_task_status(id_task, controller = obj), TASK_COMPLETE)
})


test_that("PAUSE: workers ignore tasks", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- rrq_message_send("PAUSE", controller = obj)
  w$step(TRUE)

  expect_equal(rrq_worker_status(controller = obj),
               set_names(WORKER_PAUSED, w$id))

  task <- rrq_task_create_expr(sin(1), controller = obj)
  expect_silent(w$step(TRUE))

  expect_equal(rrq_task_status(task, controller = obj), TASK_PENDING)

  id <- rrq_message_send("RESUME", controller = obj)
  w$step(TRUE)
  expect_equal(rrq_worker_status(controller = obj),
               set_names(WORKER_IDLE, w$id))

  expect_equal(rrq_task_status(task, controller = obj), TASK_PENDING)

  w$step(TRUE)
  expect_equal(rrq_task_status(task, controller = obj), TASK_COMPLETE)
})


test_that("PAUSE: workers accept messages", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- rrq_message_send("PAUSE", controller = obj)
  w$step(TRUE)

  id <- rrq_message_send("PING", controller = obj)
  expect_message(w$step(TRUE), "PONG")

  id <- rrq_message_send("STOP", controller = obj)
  expect_error(w$step(TRUE), "BYE", class = "rrq_worker_stop")
})


test_that("PAUSE/RESUME: twice is noop", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id1 <- rrq_message_send("RESUME", controller = obj)
  w$step(TRUE)

  expect_equal(rrq_message_get_response(id1, w$id, controller = obj),
               set_names(list("NOOP"), w$id))

  id2 <- rrq_message_send("PAUSE", controller = obj)
  id3 <- rrq_message_send("PAUSE", controller = obj)
  w$step(TRUE)
  w$step(TRUE)

  expect_equal(rrq_message_get_response(id3, w$id, controller = obj),
               set_names(list("NOOP"), w$id))
})


test_that("REFRESH", {
  root <- tempfile()
  obj <- test_rrq("myfuns.R", root)
  myfuns <- file.path(root, "myfuns.R")

  w <- test_worker_blocking(obj)

  t1 <- rrq_task_create_expr(f1(1), controller = obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(t1, controller = obj), 2)

  writeLines("f1 <- function(x) x + 2", myfuns)
  t2 <- rrq_task_create_expr(f1(2), controller = obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(t2, controller = obj), 3)

  id <- rrq_message_send("REFRESH", controller = obj)
  w$step(TRUE)
  expect_equal(rrq_message_get_response(id, w$id, FALSE, controller = obj),
               list("OK"))

  t3 <- rrq_task_create_expr(f1(3), controller = obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(t3, controller = obj), 5)
})


test_that("unknown command", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- rrq_message_send("XXXX", controller = obj)
  expect_message(
    w$step(TRUE),
    "Recieved unknown message: [XXXX]", fixed = TRUE)

  res <- rrq_message_get_response(id, w$id, timeout = 1, controller = obj)[[1]]
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "XXXX")
  expect_null(res$args)
})


test_that("unknown command with arguments", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- rrq_message_send("XXXX", "YYYY", controller = obj)
  expect_message(
    w$step(TRUE),
    "Recieved unknown message: [XXXX]", fixed = TRUE)

  res <- rrq_message_get_response(id, w$id, timeout = 1, controller = obj)[[1]]
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "XXXX")
  expect_equal(res$args, "YYYY")
})


test_that("unknown command with complex arguments", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  id <- rrq_message_send("XXXX", mtcars, controller = obj)
  expect_message(
    w$step(TRUE),
    "Recieved unknown message: [XXXX]", fixed = TRUE)

  res <- rrq_message_get_response(id, w$id, timeout = 1, controller = obj)[[1]]
  expect_match(res$message, "Recieved unknown message")
  expect_equal(res$command, "XXXX")
  expect_equal(res$args, mtcars)
})


test_that("send and wait", {
  obj <- test_rrq()

  w <- test_worker_spawn(obj, 5)

  st <- rrq_worker_status(controller = obj)
  expect_equal(sort(names(st)), sort(w$id))
  expect_equal(unname(st), rep(WORKER_IDLE, length(w$id)))

  res <- rrq_message_send_and_wait("PING", controller = obj)
  expect_equal(sort(names(res)), sort(w$id))
  expect_equal(unname(res), rep(list("PONG"), length(w$id)))

  ## Send to just one worker:
  res <- rrq_message_send_and_wait("PING", worker_ids = w$id[[1]],
                                   controller = obj)
  expect_equal(res, set_names(list("PONG"), w$id[[1]]))

  ## Don't delete:
  res <- rrq_message_send_and_wait("PING", worker_ids = w$id[[1]],
                                   delete = FALSE, controller = obj)

  expect_equal(res[[1]], "PONG")
  expect_equal(names(res), w$id[[1]])
  id <- attr(res, "message_id")
  expect_type(id, "character")
  expect_equal(rrq_message_get_response(id, w$id[[1]], controller = obj),
               set_names(list("PONG"), w$id[[1]]))

  res <- rrq_message_send_and_wait("STOP", controller = obj)
  expect_equal(sort(names(res)), sort(w$id))
  expect_equal(unname(res), rep(list("BYE"), length(w$id)))
})


test_that("Some messages reset timer", {
  obj <- test_rrq()
  w <- test_worker_blocking(obj)

  rrq_message_send("TIMEOUT_SET", 100, controller = obj)
  w$step(TRUE)
  expect_is_function(r6_private(w)$timer)

  id <- rrq_message_send("TIMEOUT_GET", controller = obj)
  w$step(TRUE)
  expect_is_function(r6_private(w)$timer)

  f <- function(cmd, args = NULL) {
    rrq_message_send("TIMEOUT_SET", 100, controller = obj)
    w$step(TRUE)
    stopifnot(is.function(r6_private(w)$timer))
    id <- rrq_message_send(cmd, args, controller = obj)
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
