context("utils")

test_that("rstrip", {
  expect_equal(rstrip("  "), "")
  expect_equal(rstrip("a  "), "a")
  expect_equal(rstrip("  a  "), "  a")
  expect_equal(rstrip("  a "), "  a")
})


test_that("assertions", {
  expect_error(assert_scalar(NULL), "must be a scalar")
  expect_error(assert_scalar(1:2), "must be a scalar")
  expect_error(assert_scalar(character(0)), "must be a scalar")

  expect_error(assert_character(1L), "must be character")
  expect_error(assert_character(pi), "must be character")
  expect_silent(assert_character("a"))

  expect_error(assert_character_or_null(1L), "must be character")
  expect_error(assert_character_or_null(pi), "must be character")
  expect_silent(assert_character_or_null(NULL))

  expect_error(assert_length(1:3, 2), "must be length 2")

  expect_error(assert_integer_like(pi), "must be integer like")

  expect_error(assert_is(1, "R6"), "must inherit from R6")
})


test_that("version_string", {
  dat <- version_info("R6")
  expect_match(version_string(dat),
               sprintf("%s \\[.*\\]$", dat$version))
  dat$repository <- NULL
  expect_match(version_string(dat),
               sprintf("%s \\[LOCAL\\]$", dat$version))
  dat$sha <- "aaa"
  expect_match(version_string(dat),
               sprintf("%s \\[aaa\\]$", dat$version))
})


test_that("bin_to_object_safe", {
  d <- runif(10)
  x <- object_to_bin(d)
  expect_equal(bin_to_object_safe(x), d)

  expect_null(bin_to_object_safe(NULL))
})


test_that("Sys_getenv", {
  key <- sprintf("RRQ_%s", ids::random_id())
  do.call("Sys.setenv", set_names(list("true"), key))
  expect_equal(sys_getenv(key), "true")
  Sys.unsetenv(key)
  expect_error(sys_getenv(key), "Environment variable 'RRQ_.*' not set")
})


test_that("polling: simple case", {
  set.seed(1)
  n <- sample(10, 10, replace = TRUE)
  counter <- make_counter()
  fetch <- function() {
    counter() >= n
  }

  expect_equal(
    general_poll(fetch, 0, 10, "things", FALSE, FALSE),
    rep(TRUE, 10))
  expect_equal(environment(counter)$n, max(n))
})


test_that("polling: behaviour if incomplete", {
  set.seed(1)
  n <- sample(10, 10, replace = TRUE)
  counter <- make_counter(4)
  fetch <- function() {
    counter() >= n
  }

  expect_error(
    general_poll(fetch, 0, 0, "things", TRUE, FALSE),
    sprintf("Exceeded maximum time (%s / 10 things pending)",
            sum(n > 5)),
    fixed = TRUE)

  expect_equal(
    general_poll(fetch, 0, 0, "things", FALSE, FALSE),
    n <= 6)
})


test_that("polling: behaviour if timeout", {
  set.seed(1)
  n <- sample(10, 10, replace = FALSE)
  counter <- make_counter(0)
  fetch <- function() {
    counter() >= n
  }
  poll <- 0.05
  expect_error(
    general_poll(fetch, poll, poll * 3, "things", TRUE, FALSE),
    "Exceeded maximum time")
})
