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


test_that("lapply with anonymous function", {
  obj <- test_rrq()
  obj$worker_config_save("localhost", verbose = FALSE, timeout = -1,
                         time_poll = 1, overwrite = TRUE)
  w <- test_worker_blocking(obj)
  grp <- obj$lapply_(1:10, function(x) x + 1, timeout = 0)
  w$loop(immediate = TRUE)
  res <- obj$bulk_wait(grp)
  expect_equal(res, as.list(2:11))
})


test_that("NSE - use namespaced function", {
  obj <- test_rrq()
  obj$worker_config_save("localhost", verbose = FALSE, timeout = -1,
                         time_poll = 1, overwrite = TRUE)
  w <- test_worker_blocking(obj)
  grp <- obj$lapply(1:10, ids::adjective_animal, timeout = 0)

  dat <- bin_to_object(obj$con$HGET(obj$keys$task_expr, grp$task_ids[[1]]))
  expect_equal(dat, list(expr = quote(ids::adjective_animal(1L))))

  w$loop(immediate = TRUE)
  res <- obj$bulk_wait(grp)
  expect_equal(lengths(res), 1:10)
  expect_match(unlist(res), "^[a-z]+_[a-z]+$")
})


test_that("NSE - use namespaced function with lazy dots", {
  obj <- test_rrq()
  obj$worker_config_save("localhost", verbose = FALSE, timeout = -1,
                         time_poll = 1, overwrite = TRUE)
  w <- test_worker_blocking(obj)
  mystyle <- "camel"
  grp <- obj$lapply(1:10, ids::adjective_animal, style = mystyle, timeout = 0)

  dat <- bin_to_object(obj$con$HGET(obj$keys$task_expr, grp$task_ids[[1]]))
  expect_equal(dat,
               list(expr = quote(ids::adjective_animal(1L, style = mystyle)),
                    objects = c(mystyle = obj$db$hash_object(mystyle))))

  w$loop(immediate = TRUE)
  res <- obj$bulk_wait(grp)
  expect_equal(lengths(res), 1:10)
  expect_match(unlist(res), "^[a-z]+[A-Z][a-z]+$")
})
