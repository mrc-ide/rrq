context("expression")

test_that("eval safely - simple case", {
  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  expect_equal(
    expression_eval_safely(quote(a + b), e),
    list(value = 3, success = TRUE, warnings = character(0)))
})


test_that("eval safely - error", {
  f1 <- function(x) f2(x)
  f2 <- function(x) f3(x)
  f3 <- function(x) f4(x)
  f4 <- function(x) {
    stop("some deep error")
  }

  res <- expression_eval_safely(f1(FALSE), e)
  expect_false(res$success)
  expect_is(res$value, "rrq_task_error")
  expect_is(res$value, "error")
  expect_equal(res$value$message, "some deep error")
  expect_is(res$value$trace, "character")
  expect_match(res$value$trace, "f3(x)", fixed = TRUE, all = FALSE)
})


test_that("eval safely - collect warnings", {
  f <- function(x) {
    for (i in seq_len(x)) {
      warning(sprintf("This is warning number %d", i))
    }
    x
  }

  suppressWarnings(
    res <- expression_eval_safely(f(4), new.env(parent = baseenv())))
  expect_equal(res$warnings, sprintf("This is warning number %d", 1:4))
  expect_true(res$success)
  expect_equal(res$value, 4)
})
