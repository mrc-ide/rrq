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
  obj <- test_rrq("myfuns.R")
  root <- obj$context$root$path
  unlink(file.path(root, "myfuns.R"))

  dat <- evaluate_promise(
    try(test_worker_spawn(obj, 2, timeout = 2),
        silent = TRUE))

  expect_is(dat$result, "try-error")
  expect_match(dat$messages, "2 / 2 workers not identified in time",
               all = FALSE, fixed = TRUE)
  expect_match(dat$messages, "Log files recovered for 2 workers",
               all = FALSE, fixed = TRUE)
  ## This might be failing occasionally under covr, but I can't
  ## reproduce
  expect_match(dat$output, "No such file or directory",
               all = FALSE, fixed = TRUE)
})
