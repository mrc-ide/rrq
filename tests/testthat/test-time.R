test_that("time_checker", {
  t <- time_checker(100)
  expect_gt(t(), 0)

  t <- time_checker(0)
  expect_lte(t(), 0)
})

test_that("time_checker - infinite time", {
  t <- time_checker(Inf)
  expect_equal(t(), Inf)
})


test_that("show_progress", {
  with_options(list(rrq.progress = TRUE), {
    expect_true(show_progress(NULL))
    expect_true(show_progress(TRUE))
    expect_false(show_progress(FALSE))
  })

  with_options(list(rrq.progress = FALSE), {
    expect_false(show_progress(NULL))
    expect_true(show_progress(TRUE))
    expect_false(show_progress(FALSE))
  })

  with_options(list(rrq.progress = NULL), {
    expect_equal(show_progress(NULL), interactive())
    expect_true(show_progress(TRUE))
    expect_false(show_progress(FALSE))
  })
})


test_that("progress - vector and with timeout", {
  skip_on_cran() # too dependent on progress internals
  p <- progress_timeout(10, show = TRUE, label = "things", timeout = 5,
                        width = 50, force = TRUE)
  expect_setequal(names(p), c("tick", "terminate"))
  expect_type(p$tick, "function")
  expect_type(p$terminate, "function")

  res1 <- evaluate_promise(p$tick(1))
  expect_equal(res1$result, FALSE)
  expect_match(res1$messages[[2]],
               "(-) [>------------]  10% things | giving up in",
               fixed = TRUE)

  res2 <- evaluate_promise(p$tick(4))
  expect_equal(res2$result, FALSE)
  expect_match(res2$messages[[2]],
               "(\\) [=====>-------]  50% things | giving up in",
               fixed = TRUE)

  res3 <- evaluate_promise(p$tick(5))
  expect_match(res3$messages[[2]],
               "(|) [=============] 100% things | giving up in",
               fixed = TRUE)

  res4 <- evaluate_promise(p$terminate())
  expect_match(res4$messages, "\r")
})


test_that("progress - single and infinite", {
  skip_on_cran() # too dependent on progress internals
  p <- progress_timeout(1, show = TRUE, label = "things", timeout = Inf,
                        width = 50, force = TRUE)

  res1 <- evaluate_promise(p$tick())
  expect_equal(res1$result, FALSE)
  expect_match(res1$messages[[2]],
               "(-) waiting for thing | waited for",
               fixed = TRUE)

  res2 <- evaluate_promise(p$tick())
  expect_equal(res2$result, FALSE)
  expect_match(res2$messages[[2]],
               "(\\) waiting for thing | waited for",
               fixed = TRUE)
})


test_that("progress - don't show", {
  p <- progress_timeout(1, show = FALSE, label = "things", timeout = Inf,
                        width = 50, force = TRUE)
  expect_setequal(names(p), c("tick", "terminate"))
  expect_type(p$tick, "function")
  expect_type(p$terminate, "function")
  expect_silent(p$tick())
  expect_false(p$tick())
  expect_silent(p$terminate())
})


test_that("progress - timeout", {
  p <- progress_timeout(1, show = FALSE, label = "things", timeout = 0,
                        width = 50, force = TRUE)
  expect_silent(p$tick())
  expect_true(p$tick())
})


test_that("status change timeout", {
  obj <- test_rrq()
  t <- obj$enqueue(identity(1))
  expect_error(
    wait_status_change(obj$con, obj$keys, t, TASK_PENDING, 0.01, 0.005),
    "Did not change status from 'PENDING' in time")
  expect_silent(
    wait_status_change(obj$con, obj$keys, t, TASK_RUNNING, 0.01, 0.005))
})
