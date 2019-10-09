context("bulk")

test_that("lapply simple case", {
  obj <- test_rrq()
  obj$worker_config_save("localhost", verbose = FALSE, timeout = -1,
                         time_poll = 1, overwrite = TRUE)
  w <- test_worker_blocking(obj)

  grp <- obj$lapply_(1:10, quote(log), DOTS = list(base = 2), timeout = 0)
  expect_is(grp, "rrq_bulk")
  expect_setequal(names(grp), c("task_ids", "key_complete", "names"))

  w$loop(immediate = TRUE)

  res <- obj$bulk_wait(grp)
  expect_equal(res, as.list(log2(1:10)))

  ## tasks are deleted on collection
  expect_equal(obj$task_status(grp$task_ids),
               set_names(rep(TASK_MISSING, 10), grp$task_ids))

  log <- obj$worker_log_tail(w$name, Inf)
  expect_equal(log$command,
               c("ALIVE",
                 rep(c("TASK_START", "TASK_COMPLETE"), 10),
                 "STOP"))
})
