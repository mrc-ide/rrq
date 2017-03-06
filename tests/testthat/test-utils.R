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
