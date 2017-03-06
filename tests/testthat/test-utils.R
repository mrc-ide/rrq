context("utils")

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
