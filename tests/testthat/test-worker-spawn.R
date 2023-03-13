test_that("Don't wait", {
  obj <- test_rrq()
  res <- test_worker_spawn(obj, 4, timeout = 0)
  expect_type(res$names, "character")
  expect_match(res$names, "_[0-9]+$")
  expect_type(res$key_alive, "character")

  ans <- rrq_worker_wait(obj, res$key_alive, timeout = 10, time_poll = 1)
  expect_setequal(ans, res$names)

  ans <- rrq_worker_wait(obj, res$key_alive, timeout = 10, time_poll = 1)
  expect_equal(ans, res$names)
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
  wid <- test_worker_spawn(obj, 1)
  obj$message_send_and_wait("STOP")
  txt <- obj$worker_process_log(wid)
  expect_type(txt, "character")
  expect_match(txt, "ALIVE", all = FALSE)
})


test_that("wait for worker exit", {
  obj <- test_rrq("myfuns.R", timeout_worker_stop = 0)
  wid <- test_worker_spawn(obj)

  con <- obj$con # save a copy
  queue_id <- obj$queue_id
  obj$destroy(timeout_worker_stop = 0.5)

  expect_equal(
    redux::scan_find(con, paste0(queue_id, ":*")),
    character(0))
})
