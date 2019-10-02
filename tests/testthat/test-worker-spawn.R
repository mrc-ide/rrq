context("worker spawn")

test_that("Don't wait", {
  obj <- test_rrq()
  res <- test_worker_spawn(obj, 4, timeout = 0)
  expect_is(res$names, "character")
  expect_match(res$names, "_[0-9]+$")
  expect_is(res$key_alive, "character")

  ans <- worker_wait(obj, res$key_alive, timeout = 10, time_poll = 1)
  expect_setequal(ans, res$names)

  ans <- worker_wait(obj, res$key_alive, timeout = 10, time_poll = 1)
  expect_equal(ans, res$names)
})


test_that("failed spawn", {
  root <- tempfile()
  obj <- test_rrq("myfuns.R", root)
  unlink(file.path(root, "myfuns.R"))

  dat <- evaluate_promise(
    try(test_worker_spawn(obj, 2, timeout = 2),
        silent = TRUE))

  expect_is(dat$result, "try-error")
  expect_match(dat$messages, "2 / 2 workers not identified in time",
               all = FALSE, fixed = TRUE)
  expect_match(dat$output, "Log files recovered for 2 workers",
               all = FALSE, fixed = TRUE)
  ## This fails occasionally under covr, but I can't reproduce
  ## expect_match(dat$output, "No such file or directory",
  ##              all = FALSE, fixed = TRUE)
})


test_that("read worker process log", {
  obj <- test_rrq()
  wid <- test_worker_spawn(obj, 1)
  obj$message_send_and_wait("STOP")
  txt <- obj$worker_process_log(wid)
  expect_is(txt, "character")
  expect_match(txt, "ALIVE", all = FALSE)
})


test_that("wait for worker exit", {
  obj <- test_rrq("myfuns.R")
  wid <- test_worker_spawn(obj)

  con <- obj$con # save a copy
  queue_id <- obj$queue_id
  obj$destroy(worker_stop_timeout = 0.5)

  expect_equal(
    redux::scan_find(con, paste0(queue_id, ":*")),
    character(0))
})
