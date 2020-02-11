context("bulk support")

test_that("match_fun_envir can find functions by name", {
  add <- function(a, b) a + b
  expected1 <- list(name = NULL, value = add)
  expected2 <- list(name = quote(add), value = add)
  expect_equal(match_fun_envir("add"), expected1)
  expect_equal(match_fun_envir(quote(add)), expected1)
  expect_equal(match_fun_envir("add", envir_base = environment()),
               expected2)
  expect_equal(match_fun_envir(quote(quote(add)), envir_base = environment()),
               expected2)
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

  expect_equal(match_fun_envir(quote(add), e2, e1),
               list(name = quote(add), value = add))
  expect_equal(match_fun_envir(quote(sum), e2, e1),
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