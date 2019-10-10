context("expression")

test_that("eval safely - simple case", {
  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  expect_equal(
    expression_eval_safely(quote(a + b), e),
    list(value = 3, success = TRUE))
})


test_that("eval safely - error", {
  f1 <- function(x) f2(x)
  f2 <- function(x) f3(x)
  f3 <- function(x) f4(x)
  f4 <- function(x) {
    stop("some deep error")
  }

  res <- expression_eval_safely(f1(FALSE), e)
  expect_false(res$success)
  expect_is(res$value, "rrq_task_error")
  expect_is(res$value, "error")
  expect_equal(res$value$message, "some deep error")
  expect_is(res$value$trace, "character")
  expect_match(res$value$trace, "f3(x)", fixed = TRUE, all = FALSE)
})


test_that("eval safely - collect warnings", {
  f <- function(x) {
    for (i in seq_len(x)) {
      warning(sprintf("This is warning number %d", i))
    }
    stop("giving up now")
  }

  suppressWarnings(
    res <- expression_eval_safely(f(4), new.env(parent = baseenv())))
  expect_equal(res$value$warnings, sprintf("This is warning number %d", 1:4))
  expect_false(res$success)
})


test_that("store expression", {
  db <- storr::storr_environment()
  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  res <- expression_prepare(quote(sin(1)), e, NULL, db)
  expect_equal(res, list(expr = quote(sin(1))))
  expect_equal(db$list(), character(0))
})


test_that("store locals", {
  db <- storr::storr_environment()
  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  res <- expression_prepare(quote(sin(a) + cos(b)), e, NULL, db)

  h <- c(a = db$hash_object(1), b = db$hash_object(2))
  expect_equal(res$expr, quote(sin(a) + cos(b)))
  expect_equal(res$objects, h)
  expect_equal(sort(db$list()), sort(unname(h)))
  expect_equal(db$mget(h), list(1, 2))
})


test_that("skip analysis", {
  db <- storr::storr_environment()
  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  res <- expression_prepare(quote(sin(a) + cos(b)), e, NULL, db,
                            analyse = FALSE)
  expect_equal(res, list(expr = quote(sin(a) + cos(b))))
  expect_equal(db$list(), character(0))
})


test_that("add variables", {
  db <- storr::storr_environment()
  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  res <- expression_prepare(quote(sin(a) + cos(b)), e, NULL, db,
                            analyse = FALSE, include = "a")

  h <- db$hash_object(1)
  expect_equal(res$expr, quote(sin(a) + cos(b)))
  expect_equal(res$objects, c(a = h))
  expect_equal(db$list(), h)
  expect_equal(db$get(h), 1)
})


test_that("exclude variables", {
  db <- storr::storr_environment()
  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  res <- expression_prepare(quote(sin(a) + cos(b)), e, NULL, db,
                            exclude = "a")

  h <- db$hash_object(2)
  expect_equal(res$expr, quote(sin(a) + cos(b)))
  expect_equal(res$objects, c(b = h))
  expect_equal(db$list(), h)
  expect_equal(db$get(h), 2)
})


test_that("recognise base variables", {
  db <- storr::storr_environment()
  e1 <- list2env(list(a = 1), parent = baseenv())
  e2 <- list2env(list(b = 2), parent = baseenv())
  e3 <- new.env(parent = e2)

  res <- expression_prepare(quote(sin(a) + cos(b)), e1, e2, db)

  h <- db$hash_object(1)
  expect_equal(res$expr, quote(sin(a) + cos(b)))
  expect_equal(res$objects, c(a = h))
  expect_equal(db$list(), h)
  expect_equal(db$get(h), 1)

  expect_equal(expression_prepare(quote(sin(a) + cos(b)), e1, e3, db), res)
  expect_error(expression_prepare(quote(sin(a) + cos(b)), e1, e1, db),
               "not all objects found: b")
})


test_that("restore locals", {
  e <- new.env()
  db <- storr::storr_environment()
  h1 <- db$set_by_value(1)
  h2 <- db$set_by_value(2)

  res <- expression_restore_locals(list(objects = c(a = h1)), e, db)
  expect_equal(ls(res), "a")
  expect_equal(res$a, 1)
  expect_identical(parent.env(res), e)
})


test_that("can store special function values", {
  db <- storr::storr_environment()
  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  f <- function(a, b) f(a + b)
  ## Can't compute the hash with db$hash_object because we get a
  ## different one each time in a local environment due to the local
  ## environment.
  res <- expression_prepare(quote(.(a, b)), e, NULL, db,
                            function_value = f)
  expect_match(res$function_hash, "^[[:xdigit:]]+$")
  e2 <- expression_restore_locals(res, emptyenv(), db)
  expect_equal(sort(names(e2)), sort(c("a", "b", res$function_hash)))
  expect_equal(e2[[res$function_hash]], f)
  expect_equal(e2$a, 1)
  expect_equal(e2$b, 2)
})


test_that("can restore function even with no variables", {
  db <- storr::storr_environment()
  e <- emptyenv()
  f <- function(a, b) f(a + b)
  res <- expression_prepare(quote(.(1, 2)), e, NULL, db,
                            function_value = f)
  expect_match(res$function_hash, "^[[:xdigit:]]+$")
  e2 <- expression_restore_locals(res, emptyenv(), db)
  expect_equal(names(e2), res$function_hash)
  expect_equal(e2[[res$function_hash]], f)
  expect_equal(deparse(res$expr[[1]]), res$function_hash)
})


test_that("require a call to prepre", {
  db <- storr::storr_environment()
  expect_error(expression_prepare(quote(a), emptyenv(), NULL, db),
               "Expected a call")
  expect_error(expression_prepare(quote(1), emptyenv(), NULL, db),
               "Expected a call")
})
