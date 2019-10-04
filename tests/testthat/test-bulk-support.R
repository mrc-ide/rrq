context("bulk support")

test_that("match_fun_envir can find functions by name", {
  add <- function(a, b) a + b
  expected <- list(value = add, type = "value")
  expect_equal(match_fun_envir(add), expected)
  expect_equal(match_fun_envir("add"), expected)
  expect_equal(match_fun_envir(quote(add)), expected)
})


test_that("match_fun_envir can detect shared functions", {
  add <- function(a, b) a + b
  base <- list2env(list(add = add), parent = emptyenv())
  expected <- list(value = quote(add), type = "name")
  expect_equal(match_fun_envir(add, envir_base = base), expected)
  expect_equal(match_fun_envir("add", envir_base = base), expected)
  expect_equal(match_fun_envir(quote(add), envir_base = base), expected)
})


test_that("match_fun_envir can deal with namespaced functions", {
  expected <- list(value = quote(ids::random_id), type = "name")
  expect_equal(match_fun_envir(ids::random_id), expected)
  expect_equal(match_fun_envir(quote(ids::random_id)), expected)
})


test_that("match_fun_envir can deal with hidden functions", {
  expected <- list(value = quote(ids:::as_integer_bignum), type = "name")
  expect_equal(match_fun_envir(ids:::as_integer_bignum), expected)
  expect_equal(match_fun_envir(quote(ids:::as_integer_bignum)), expected)
})


test_that("match_fun_envir can deal with anonymous functions", {
  res <- match_fun_envir(function(a, b) a + b)
  expect_equal(res$type, "value")
  expect_is(res$value, "function")
  expect_equal(body(res$value), quote(a + b))
  expect_equal(res$value(1, 2), 3)
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
