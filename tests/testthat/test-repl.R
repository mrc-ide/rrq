context("repl")

test_that("Can evaluate and collect all output", {
  skip_on_cran()
  path <- tempfile()
  res <- withr::with_connection(
    list(con = file(path, "w")),
    repl_eval(quote(1 + 2), globalenv(), con))
  expect_true(res$success)
  expect_equal(res$warnings, list())
  expect_equal(res$value, list(value = 3, visible = TRUE))
  expect_equal(
    readLines(path),
    capture.output(print(3)))
})


test_that("Can evaluate and return errors", {
  skip_on_cran()
  testthat::local_edition(3)
  env <- environment()

  f <- function(x) {
    g(x)
  }
  g <- function(x) {
    message("calculating!")
    h(x)
  }
  h <- function(x) {
    if (x < 0) {
      stop("x must be positive")
    } else if (x > 10) {
      stop(structure(list(message = "x must be less than 10"),
                     class = c("custom_error", "error", "condition")))
    }
    x
  }

  path <- tempfile()
  res <- withr::with_connection(
    list(con = file(path, "w")),
    repl_eval(f(-1), env, con))

  expect_equal(res$trace[[1]], quote(f(-1)))
  expect_equal(res$error,
               with(list(x = -1), tryCatch(h(x), error = identity)))
  expect_equal(readLines(path),
               c("calculating!", "Error in h(x): x must be positive"))
})


test_that("can evaluate and interleave stdout and stderr", {
  ## This works badly because testthat sets suppressMessages, which
  ## means that the messages are never emitted. This is fixed in the
  ## 3rd edition of testthat.
  skip_on_cran()
  testthat::local_edition(3)
  path <- tempfile()
  res <- withr::with_connection(
    list(con = file(path, "w")),
    repl_eval(quote({
      message("1")
      cat("2\n")
      message("3")
      cat("4\n")
      cat("5\n")
      message("6")
    }), globalenv(), con))
  .GlobalEnv$.res <- res
  .GlobalEnv$.contents <- readLines(path)
  expect_equal(readLines(path),
               as.character(1:6))
})


test_that("Can evaluate and return warnings", {
  skip_on_cran()
  path <- tempfile()

  f <- function(x) {
    g(x)
  }
  g <- function(x) {
    for (i in seq_len(abs(x))) {
      warning("Some warning ", i)
    }
    if (x < 0) {
      stop("x must be positive")
    }
    x
  }

  env <- environment()
  res <- withr::with_connection(
    list(con = file(path, "w")),
    repl_eval(quote(f(3)), env, con))
  expect_equal(
    readLines(path),
    c("[1] 3",
      "Warning messages:",
      "1: Some warning 1",
      "2: Some warning 2",
      "3: Some warning 3"))
  expect_equal(res$value, list(value = 3, visible = TRUE))
})
