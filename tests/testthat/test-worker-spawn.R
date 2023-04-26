test_that("Don't wait", {
  obj <- test_rrq()
  res <- test_worker_spawn(obj, 4, timeout = 0)

  expect_s3_class(res, "rrq_worker_manager")
  expect_type(res$id, "character")
  expect_match(res$id, "_[0-9]+$")

  ans <- withVisible(res$wait_alive(timeout = 10, time_poll = 1))
  expect_false(ans$visible)
  expect_s3_class(ans$value, "difftime")

  ## Can call again with no ill effect:
  expect_s3_class(
    res$wait_alive(timeout = 10, time_poll = 1),
    "difftime")
})


test_that("failed spawn", {
  skip_on_windows()

  root <- tempfile()
  obj <- test_rrq("myfuns.R", root, verbose = TRUE)
  unlink(file.path(root, "myfuns.R"))

  dat <- evaluate_promise(
    try(rrq_worker_spawn(obj, 2, timeout = 2),
        silent = TRUE))

  expect_s3_class(dat$result, "try-error")
  expect_match(dat$messages, "2 / 2 workers not identified in time",
               all = FALSE, fixed = TRUE)
  expect_match(dat$output, "Log files recovered for 2 workers",
               all = FALSE, fixed = TRUE)
  ## This fails occasionally under covr, but I can't reproduce
  ## expect_match(dat$output, "No such file or directory",
  ##              all = FALSE, fixed = TRUE)
})


test_that("read worker process log", {
  obj <- test_rrq(verbose = TRUE)
  w <- test_worker_spawn(obj, 1)
  obj$message_send_and_wait("STOP")
  txt <- obj$worker_process_log(w$id)
  expect_type(txt, "character")
  expect_match(txt, "ALIVE", all = FALSE)
  expect_equal(txt, w$logs(1))
})


test_that("wait for worker exit", {
  obj <- test_rrq("myfuns.R", timeout_worker_stop = 0)
  w <- test_worker_spawn(obj)

  con <- obj$con # save a copy
  queue_id <- obj$queue_id
  obj$destroy(timeout_worker_stop = 0.5)

  expect_equal(
    redux::scan_find(con, paste0(queue_id, ":*")),
    character(0))
})


test_that("error if we try to interact with non-managed worker", {
  obj <- test_rrq("myfuns.R")
  w <- test_worker_spawn(obj)
  expect_error(
    w$logs(2),
    "Worker not controlled by this manager: '.+_2'")
  expect_error(
    w$logs("fred"),
    "Worker not controlled by this manager: 'fred'")
})


test_that("can wait on manually spawned workers", {
  obj <- test_rrq("myfuns.R")

  queue_id <- obj$queue_id
  worker_ids <- sprintf("%s_%d", ids::adjective_animal(), 1:2)
  key_alive <- rrq::rrq_worker_expect(obj, worker_ids)
  expect_type(key_alive, "character")
  expect_length(key_alive, 1)
  expect_match(key_alive,
               "^rrq:[[:xdigit:]]{32}:worker:alive:[[:xdigit:]]{32}$")

  bin <- obj$con$HGET(rrq_keys(queue_id)$worker_expect, key_alive)
  expect_type(bin, "raw")
  expect_equal(bin_to_object(bin), worker_ids)

  expect_error(
    suppressMessages(rrq_worker_wait(obj, key_alive, 0, 1, FALSE)),
    "Not all workers recovered")

  p1 <- callr::r_bg(function(queue_id, worker_id) {
    rrq::rrq_worker$new(queue_id, worker_id = worker_id)$loop()
  }, list(queue_id, worker_ids[[1]]), package = TRUE, cleanup = FALSE)
  p2 <- callr::r_bg(function(queue_id, worker_id) {
    rrq::rrq_worker$new(queue_id, worker_id = worker_id)$loop()
  }, list(queue_id, worker_ids[[2]]), package = TRUE, cleanup = FALSE)

  res <- rrq_worker_wait(obj, key_alive, 5, 1, FALSE)
  expect_s3_class(res, "difftime")

  ## If we're unlucky GC will happen within a loop here, so be kind
  ## and try a couple of times. Potentially problemetic as part of the
  ## coverage build, which is very slow.
  testthat::try_again(
    5,
    expect_lt(rrq_worker_wait(obj, key_alive, 5, 1, FALSE), 0.5))
})


test_that("error if no workers expected on key", {
  obj <- test_rrq("myfuns.R")
  key_alive <- rrq_key_worker_alive(obj$queue_id)
  expect_error(
    rrq_worker_wait(obj, key_alive, 0, 1, FALSE),
    "No workers expected on that key")
})
