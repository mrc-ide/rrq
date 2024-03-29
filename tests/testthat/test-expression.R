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

  e <- new.env()
  res <- expression_eval_safely(f1(FALSE), e)
  expect_false(res$success)
  expect_s3_class(res$value, "error")
  expect_equal(res$value$message, "some deep error")
  expect_s3_class(res$value$trace, "rlang_trace")
  expect_gt(nrow(res$value$trace), 4)
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
  store <- test_store()
  tag <- ids::random_id()

  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  res <- expression_prepare(quote(sin(1)), e, store, tag)

  expect_equal(res, list(expr = quote(sin(1))))
  expect_equal(store$list(), character(0))
})


test_that("store locals", {
  store <- test_store()
  tag <- ids::random_id()

  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  res <- expression_prepare(quote(sin(a) + cos(b)), e, store, tag)

  hash_object <- function(x) {
    hash_data(object_to_bin(x))
  }
  h <- c(a = hash_object(1), b = hash_object(2))

  expect_equal(res$expr, quote(sin(a) + cos(b)))
  expect_equal(res$objects, h)
  expect_setequal(store$list(), h)
  expect_equal(store$mget(h), list(1, 2))
})


test_that("skip analysis", {
  store <- test_store()
  tag <- ids::random_id()

  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  res <- expression_prepare(quote(sin(a) + cos(b)), e, store, tag,
                            export = character(0))
  expect_equal(res, list(expr = quote(sin(a) + cos(b))))
  expect_equal(store$list(), character(0))
})


test_that("export variables", {
  store <- test_store()
  tag <- ids::random_id()

  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  res <- expression_prepare(quote(sin(a) + cos(b)), e, store, tag,
                            export = "a")
  h <- hash_data(object_to_bin(1))
  expect_equal(res$expr, quote(sin(a) + cos(b)))
  expect_equal(res$objects, c(a = h))
  expect_equal(store$list(), h)
  expect_equal(store$get(h), 1)
})


test_that("export variables", {
  store <- test_store()
  tag <- ids::random_id()

  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  res <- expression_prepare(quote(sin(a) + cos(b)), e, store, tag,
                            export = list(a = 10))
  h <- hash_data(object_to_bin(10))
  expect_equal(res$expr, quote(sin(a) + cos(b)))
  expect_equal(res$objects, c(a = h))
  expect_equal(store$list(), h)
  expect_equal(store$get(h), 10)
})


test_that("restore locals", {
  store <- test_store()
  tag <- ids::random_id()

  h1 <- store$set(1, tag)
  h2 <- store$set(2, tag)

  e <- new.env()
  res <- expression_restore_locals(list(objects = c(a = h1)), e, store)
  expect_equal(ls(res), "a")
  expect_equal(res$a, 1)
  expect_identical(parent.env(res), e)
})


test_that("can store special function values", {
  store <- test_store()
  tag <- ids::random_id()

  e <- list2env(list(a = 1, b = 2), parent = baseenv())
  f <- function(a, b) a + b
  environment(f) <- globalenv()
  ## Can't compute the hash directly because we get a different one
  ## each time in a local environment due to the local environment.
  res <- expression_prepare(quote(.(a, b)), e, store, tag,
                            function_value = f)
  expect_match(res$function_hash, "^[[:xdigit:]]+$")
  env <- new.env(parent = baseenv())
  e2 <- expression_restore_locals(res, env, store)
  expect_equal(sort(names(e2)), sort(c("a", "b", res$function_hash)))
  expect_equal(e2[[res$function_hash]](1, 2), 3)
  expect_equal(e2$a, 1)
  expect_equal(e2$b, 2)
})


test_that("can restore function even with no variables", {
  store <- test_store()

  e <- emptyenv()
  f <- function(a, b) a + b
  environment(f) <- globalenv()
  tag <- ids::random_id()

  res <- expression_prepare(quote(.(1, 2)), e, store, tag,
                            function_value = f)
  expect_match(res$function_hash, "^[[:xdigit:]]+$")
  env <- new.env(parent = baseenv())
  e2 <- expression_restore_locals(res, env, store)
  expect_equal(names(e2), res$function_hash)
  expect_equal(e2[[res$function_hash]](1, 2), 3)
  expect_equal(deparse(res$expr[[1]]), res$function_hash)
})


test_that("require a call to prepre", {
  store <- test_store()
  tag <- ids::random_id()

  expect_error(expression_prepare(quote(a), emptyenv(), store, tag),
               "Expected a call")
  expect_error(expression_prepare(quote(1), emptyenv(), store, tag),
               "Expected a call")
})
