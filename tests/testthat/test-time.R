context("time")

test_that("time_checker", {
  t <- time_checker(100, FALSE)
  expect_false(t())
  t <- time_checker(100, TRUE)
  expect_gt(t(), 0)

  t <- time_checker(0, FALSE)
  Sys.sleep(0.001)
  expect_true(t())
  t <- time_checker(0, TRUE)
  expect_lte(t(), 0)
})

test_that("time_checker - infinite time", {
  t <- time_checker(Inf, FALSE)
  expect_false(t())
  t <- time_checker(Inf, TRUE)
  expect_equal(t(), Inf)
})
