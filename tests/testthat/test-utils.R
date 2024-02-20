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

  expect_error(assert_character(1L), "must be a character")
  expect_error(assert_character(pi), "must be a character")
  expect_silent(assert_character("a"))

  expect_error(assert_numeric("a"), "must be a numeric")
  expect_error(assert_numeric(TRUE), "must be a numeric")

  expect_error(assert_nonmissing(NA), "must not be NA")
  expect_error(assert_nonmissing(NA_integer_), "must not be NA")

  expect_error(assert_integer_like(pi), "must be integer-like")

  expect_error(assert_scalar_positive_integer(-1), "must be a positive integer")
  expect_error(assert_scalar_positive_integer(0), "must be a positive integer")
  expect_silent(assert_scalar_positive_integer(0, TRUE))

  expect_error(assert_valid_timeout(-1), "must be positive")
  expect_silent(assert_valid_timeout(1))

  expect_error(assert_is(1, "R6"), "must be a 'R6'")

  expect_silent(assert_named(list(a = 1)))
  expect_error(assert_named(list(a = 1, a = 2), TRUE),
               "must have unique names")
  expect_error(assert_named(list(1, 2)),
               "must be named")
})


test_that("assert_scalar_logical", {
  expect_error(assert_scalar_logical(1), "must be a logical")
  expect_error(assert_scalar_logical(c(TRUE, FALSE)), "must be a scalar")
  expect_silent(assert_scalar_logical(TRUE))
})


test_that("bin_to_object_safe", {
  d <- runif(10)
  x <- object_to_bin(d)
  expect_equal(bin_to_object_safe(x), d)

  expect_null(bin_to_object_safe(NULL))
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


test_that("wait timeout errors informatively", {
  skip_if_not_installed("mockery")
  callback <- mockery::mock(TRUE, cycle = TRUE)
  expect_error(
    wait_timeout("my explanation", 0.1, callback),
    "Timeout: my explanation")
  expect_gt(length(mockery::mock_args(callback)), 1)
  expect_equal(mockery::mock_args(callback)[[1]], list())
})

test_that("wait success returns result", {
  callback <- mockery::mock(stop("Failure"), TRUE)
  msg <- evaluate_promise(wait_success("my explanation", 2, callback))
  expect_equal(msg$messages, "Failure\n")
  expect_true(msg$result)
})


test_that("wait success returns error message", {
  timeout <- if (interactive()) 0.1 else 1
  poll <- timeout / 2
  callback <- mockery::mock(stop("Failure"), cycle = TRUE)
  msg <- evaluate_promise(expect_error(
    wait_success("my explanation", timeout, callback, poll)))
  expect_equal(msg$messages, c("Failure\n", "Failure\n"))
  expect_equal(msg$result$message, "Timeout: my explanation")
  expect_equal(msg$result$parent$message, "Failure")
})


test_that("Hash large data", {
  skip_on_cran() # slow, possibly problematic?
  d <- raw(2^31)
  h <- hash_data(d)
  expect_equal(
    h,
    "2e414e29f36fec53be8f411a22e2539d")
})


test_that("Detect serialised objects", {
  skip_on_cran()
  expect_true(is_serialized_object(object_to_bin(NULL)))

  ## This is not true because we use non-xdr serialisation always
  expect_false(is_serialized_object(serialize(NULL, NULL, xdr = TRUE)))

  expect_false(is_serialized_object(1))
  expect_false(is_serialized_object(as.raw(0:100)))
  expect_false(is_serialized_object(as.raw(integer(0))))
  expect_false(is_serialized_object(as.raw(as.raw(c(0x42, 0x0a, 0:10)))))
})


test_that("can break a df into rows", {
  d <- data.frame(x = 1:3, y = c("a", "b", "c"))
  expect_equal(df_rows(d), list(list(x = 1, y = "a"),
                                list(x = 2, y = "b"),
                                list(x = 3, y = "c")))
  expect_equal(df_rows(d[1]), list(list(x = 1),
                                   list(x = 2),
                                   list(x = 3)))
})


test_that("can break a df with list columns into rows", {
  d <- data.frame(x = 1:3,
                  y = c("a", "b", "c"),
                  z = I(lapply(1:3, seq_len)))
  expect_equal(df_rows(d), list(list(x = 1, y = "a", z = seq_len(1)),
                                list(x = 2, y = "b", z = seq_len(2)),
                                list(x = 3, y = "c", z = seq_len(3))))
})
