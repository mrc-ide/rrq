test_that("rrq_envir default works", {
  create <- rrq_envir()
  e <- new.env(parent = baseenv())
  create(e)
  expect_equal(names(e), character(0))
})


test_that("rrq_envir accepts sources", {
  create <- rrq_envir(sources = "myfuns.R")
  e <- new.env(parent = baseenv())
  create(e)
  expect_true("noisydouble" %in% names(e))
})


test_that("rrq_envir accepts packages", {
  create <- rrq_envir(packages = c("testthat", "ids"))
  e <- new.env(parent = baseenv())

  mock_library <- mockery::mock()
  mockery::stub(create, "library", mock_library)
  create(e)
  mockery::expect_called(mock_library, 2)
  expect_equal(mockery::mock_args(mock_library),
               list(list("testthat", character.only = TRUE),
                    list("ids", character.only = TRUE)))
})


test_that("validate sources exist", {
  expect_error(rrq_envir(sources = 1), "sources must be character")
  expect_error(rrq_envir(sources = "myfun.R"), "File does not exist: 'myfun.R'")
})
