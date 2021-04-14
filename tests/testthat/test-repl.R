context("repl")

test_that("Can evaluate and collect all output", {
  skip_on_cran()
  path <- tempfile()
  res <- withr::with_connection(
    list(con = file(path, "w")),
    repl_eval(quote(1 + 2), globalenv(), con))
  expect_true(res$success)
  expect_equal(res$warnings, list())
  expect_equal(res$value, 3)
  expect_equal(
    readLines(path),
    capture.output(print(3)))
})


test_that("Can evaluate and return errors", {
  skip_on_cran()
  env <- environment()

  f <- function(x) {
    g(x)
  }
  g <- function(x) {
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

  path <- tempfile()
  res <- withr::with_connection(
    list(con = file(path, "w")),
    repl_eval(f(20), env, con))

  filter_trace(.cmp[[1]], .cmp[[2]])

})


test_that("can evaluate and return output/messages", {

})

test_that("Can evaluate and return warnings", {
  skip_on_cran()
  path <- tempfile()
  res <- withr::with_connection(
    list(con = file(path, "w")),
    repl_eval(quote({
      message("m1")
      warning("w1")
      message("m2")
      warning("w2")
      warning("w3")
      1
    }), globalenv(), con))


      1 + 2), globalenv(), con))

})
