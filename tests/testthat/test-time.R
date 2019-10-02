context("time")

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
