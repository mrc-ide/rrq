test_that("match_fun_envir can find functions by name", {
  add <- function(a, b) a + b
  expected <- list(name = NULL, value = add)
  expect_equal(match_fun_envir("add"), expected)
  expect_equal(match_fun_envir(quote(add)), expected)
  expect_equal(match_fun_envir(quote(quote(add))), expected)
  expect_equal(match_fun_envir(quote(quote(quote(add)))), expected)
})


test_that("match_fun_envir can deal with namespaced functions", {
  expected <- list(name = quote(ids::random_id), value = ids::random_id)
  expect_equal(match_fun_envir(quote(ids::random_id)), expected)
})


test_that("match_fun_envir can deal with hidden functions", {
  expected <- list(name = quote(ids:::as_integer_bignum),
                   value = ids:::as_integer_bignum)
  expect_equal(match_fun_envir(quote(ids:::as_integer_bignum)), expected)
})


test_that("match_fun_envir can deal with anonymous functions", {
  res <- match_fun_envir(function(a, b) a + b)
  expect_null(res$name)
  expect_equal(res$value, function(a, b) a + b)
})


test_that("match_fun_envir can find functions in nested environments", {
  add <- function(a, b) a + b
  e1 <- list2env(list(add = add), new.env(parent = .GlobalEnv))
  e2 <- list2env(list(sum = add), new.env(parent = e1))

  expect_equal(match_fun_envir(quote(add), e2),
               list(name = NULL, value = add))
  expect_equal(match_fun_envir(quote(sum), e2),
               list(name = NULL, value = add))
})


test_that("match_fun", {
  e <- new.env(parent = emptyenv())
  e$add <- function(a, b) a + b
  expect_identical(match_fun(e$add, e), e$add)
  expect_identical(match_fun("add", e), e$add)
  expect_error(match_fun(1, e), "Could not find function")
  expect_identical(match_fun(quote(ids::random_id), e), ids::random_id)
  expect_identical(match_fun(quote(ids:::as_integer_bignum), e),
                   ids:::as_integer_bignum)
})


test_that("prepare bulk x where it is a vector", {
  expect_equal(rrq_bulk_prepare_call_x(1), list(1))
  expect_equal(rrq_bulk_prepare_call_x(1:5), as.list(1:5))
  expect_equal(rrq_bulk_prepare_call_x(c(a = 1, b = 2)),
               list(a = 1, b = 2))
  expect_equal(rrq_bulk_prepare_call_x(list(1:5, 2:6)),
               list(1:5, 2:6))
  expect_error(
    rrq_bulk_prepare_call_x(list(a = 1:5, b = 2)),
    "Every element of 'x' must have the same length")

  nms <- letters[1:5]
  x <- list(set_names(1:5, nms),
            set_names(2:6, nms))
  expect_equal(rrq_bulk_prepare_call_x(x), x)
  expect_error(rrq_bulk_prepare_call_x(c(x, list(3:7))),
               "Elements of 'x' must have the same names")
  expect_error(rrq_bulk_prepare_call_x(list()),
               "'x' must have at least one element")
  expect_error(rrq_bulk_prepare_call_x(NULL),
               "x must be a data.frame or list")
})


test_that("prepare bulk x where it is a data.frame", {
  x <- data.frame(x = 1:2, y = 3:4)
  expect_equal(
    rrq_bulk_prepare_call_x(x),
    list(list(x = 1, y = 3), list(x = 2, y = 4)))

  expect_error(
    rrq_bulk_prepare_call_x(x[0, ]),
    "'x' must have at least one row")
  expect_error(
    rrq_bulk_prepare_call_x(x[, 0]),
    "'x' must have at least one column")
})


test_that("Prevent factors!", {
  expect_error(rrq_bulk_prepare_call_x(factor(1:4)),
               "Factors cannot be used in bulk expressions")
  expect_error(rrq_bulk_prepare_call_x(data.frame(a = 1:4, b = factor(1:4))),
               "Factors cannot be used in bulk expressions")
  expect_error(rrq_bulk_prepare_call_x(list(a = 1:4, b = factor(1:4))),
               "Factors cannot be used in bulk expressions")
})
