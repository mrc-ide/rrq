test_that("can create a basic expression task", {
  obj <- test_rrq()
  id <- rrq_task_create_expr(sqrt(2), controller = obj)

  d <- rrq_task_data(id, controller = obj)
  expect_equal(d$type, "expr")
  expect_equal(d$expr, quote(sqrt(2)))
  expect_null(d$variables)

  w <- test_worker_blocking(obj)
  w$step(TRUE)

  expect_equal(rrq_task_result(id, controller = obj), sqrt(2))
})


test_that("can create an expression task that references locals", {
  obj <- test_rrq()
  a <- 2
  id <- rrq_task_create_expr(sqrt(a), controller = obj)

  d <- rrq_task_data(id, controller = obj)
  expect_equal(d$type, "expr")
  expect_equal(d$expr, quote(sqrt(a)))
  expect_equal(d$variables, list(a = 2))

  w <- test_worker_blocking(obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(id, controller = obj), sqrt(2))
})


test_that("can create a call task from a namespaced function", {
  obj <- test_rrq()
  id <- rrq_task_create_call(base::sqrt, list(2), controller = obj)

  d <- rrq_task_data(id, controller = obj)
  expect_equal(d$type, "call")
  expect_equal(d$fn, list(name = "sqrt", namespace = "base", value = NULL))
  expect_equal(d$args, list(2))

  w <- test_worker_blocking(obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(id, controller = obj), sqrt(2))
})


test_that("can create a call task from a non-exported function", {
  obj <- test_rrq()
  id <- rrq_task_create_call(ids:::toupper_initial, list("foo"),
                             controller = obj)

  d <- rrq_task_data(id, controller = obj)
  expect_equal(d$type, "call")
  expect_equal(d$fn, list(name = NULL,
                          namespace = NULL,
                          value = quote(ids:::toupper_initial)))
  expect_equal(d$args, list("foo"))

  w <- test_worker_blocking(obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(id, controller = obj), "Foo")
})


test_that("can run a task using a function by name", {
  obj <- test_rrq()
  myfn <- sqrt
  id <- rrq_task_create_call(myfn, list(x = 2), controller = obj)

  d <- rrq_task_data(id, controller = obj)
  expect_equal(d$type, "call")
  expect_equal(d$fn, list(name = "myfn", namespace = NULL, value = myfn))
  expect_equal(d$args, list(x = 2))

  w <- test_worker_blocking(obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(id, controller = obj), sqrt(2))
})


test_that("can run a task using a function by value", {
  obj <- test_rrq()
  id <- eval(
    rrq_task_create_call(function(x) x + 1, list(2), controller = obj),
    globalenv())

  d <- rrq_task_data(id, controller = obj)
  expect_equal(d$type, "call")
  expect_null(d$fn$name)
  expect_null(d$fn$namespace)
  expect_type(d$fn$value, "closure")
  expect_equal(d$args, list(2))

  w <- test_worker_blocking(obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(id, controller = obj), 3)
})


test_that("can run task that throws an error", {
  obj <- test_rrq("myfuns.R")

  id <- rrq_task_create_expr(only_positive(-1), controller = obj)
  w <- test_worker_blocking(obj)
  w$step(TRUE)

  res <- obj$task_result(id)
  expect_s3_class(res, "rrq_task_error")
  expect_null(res$warnings)
  expect_equal(res$task_id, id)
  expect_equal(res$queue_id, obj$queue_id)
  expect_equal(res$status, TASK_ERROR)
})


test_that("task warnings are returned", {
  obj <- test_rrq("myfuns.R")

  id <- rrq_task_create_expr(warning_then_error(2), controller = obj)
  w <- test_worker_blocking(obj)
  w$step(TRUE)

  res <- obj$task_result(id)
  expect_s3_class(res, "rrq_task_error")
  expect_type(res$warnings, "list")
  expect_length(res$warnings, 2)
  expect_equal(res$warnings[[1]]$message, "This is warning number 1")
  expect_equal(res$warnings[[2]]$message, "This is warning number 2")

  expect_s3_class(res$trace, "rlang_trace")
  expect_match(format(res$trace), "warning_then_error", all = FALSE)
})


## These tests are largely ported from hipercow
test_that("can use escape hatch", {
  obj <- test_rrq()

  expr1 <- quote(sqrt(2))
  id1 <- rrq_task_create_expr(expr1, controller = obj)

  d1 <- rrq_task_data(id1, controller = obj)
  expect_equal(d1$type, "expr")
  expect_equal(d1$expr, quote(sqrt(2)))
  expect_null(d1$variables)

  ## Also works with expressions that reference variables, for simple
  ## cases at least.
  a <- 2
  expr2 <- quote(sqrt(a))
  id2 <- rrq_task_create_expr(expr2, controller = obj)

  d2 <- rrq_task_data(id2, controller = obj)
  expect_equal(d2$type, "expr")
  expect_equal(d2$expr, quote(sqrt(a)))
  expect_equal(d2$variables, list(a = 2))
})


test_that("An expression must be a call", {
  obj <- test_rrq()
  expect_error(
    rrq_task_create_expr(TRUE, controller = obj),
    "Expected 'expr' to be a function call")
})


test_that("pulling from a symbol must still be a call", {
  obj <- test_rrq()
  myexpr <- quote(sqrt)
  err <- expect_error(
    rrq_task_create_expr(myexpr, controller = obj),
    "Expected 'expr' to be a function call")
  expect_match(err$body[[1]], "You passed a symbol 'myexpr'")
})


test_that("symbols that might contain expressions must exist", {
  obj <- test_rrq()
  expect_error(
    rrq_task_create_expr(someexpr, controller = obj),
    "Could not find expression 'someexpr'")
})


test_that("error on double quote", {
  obj <- test_rrq()
  err <- expect_error(
    rrq_task_create_expr(quote(f(x)), controller = obj),
    "You have an extra layer of quote() around 'expr'",
    fixed = TRUE)
  expect_equal(
    err$body,
    c(i = "You passed 'quote(f(x))' but probably meant to pass 'f(x)'"))

  expr <- quote(quote(g(y)))
  err <- expect_error(
    rrq_task_create_expr(expr, controller = obj),
    "You have an extra layer of quote() around 'expr'",
    fixed = TRUE)
  expect_equal(
    err$body,
    c(i = "You passed 'quote(g(y))' but probably meant to pass 'g(y)'"))
})


test_that("can allow more complex expressions through", {
  obj <- test_rrq()
  id <- rrq_task_create_expr({
    x <- 3
    sqrt(x)
  }, controller = obj)
  w <- test_worker_blocking(obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(id, controller = obj), sqrt(3))
})


test_that("can resolve functions when call is namespaced", {
  expect_mapequal(check_function(rlang::quo(ids::random_id)),
                  list(name = "random_id",
                       namespace = "ids",
                       value = NULL))
})


test_that("can resolve function when function is in a package", {
  myfn <- ids::random_id
  expect_mapequal(check_function(rlang::quo(myfn)),
                  list(namespace = NULL, # the value
                       name = "myfn",
                       value = myfn))
})


test_that("symbols must resolve to functions immediately in the environment", {
  foo <- TRUE
  expect_error(check_function(rlang::quo(foo)),
               "The symbol 'foo' is not a function")
  myfn <- sqrt
  local({
    myfn <- 1:10
    expect_error(check_function(rlang::quo(myfn)),
                 "The symbol 'myfn' is not a function")
  })
})


test_that("can resolve a function when passed by value", {
  myfn <- function(x) x + 1
  expect_mapequal(check_function(rlang::quo(myfn)),
                  list(value = myfn,
                       name = "myfn",
                       namespace = NULL))
})


test_that("can resolve an anonymous function", {
  res <- check_function(rlang::quo(function(x) x + 1))
  expect_mapequal(res,
                  list(value = function(x) x + 1,
                       name = NULL,
                       namespace = NULL))
})


test_that("objects passed by value must be a function", {
  expect_error(check_function(rlang::quo(list(1, 2, 3))),
               "The value passed is not a function")
})


test_that("check arguments for call", {
  expect_equal(check_args(NULL), list())
  expect_equal(check_args(list()), list())
  expect_equal(check_args(list(1, 2)), list(1, 2))
  expect_equal(check_args(list(1, a = 2)), list(1, a = 2))
  expect_error(check_args(1),
               "Expeced a list for 'args'")
})


test_that("can find names in simple expressions", {
  expect_equal(find_vars(quote(f(1))), character(0))
  expect_equal(find_vars(quote(f(x))), "x")
  expect_setequal(find_vars(quote(f(x, y, 2, z))), c("x", "y", "z"))
  expect_equal(find_vars(quote(cls$new(x))), "x")
})


test_that("can find names in multiline expressions with assignments", {
  expect_equal(
    find_vars(quote({
      a <- 1
      f(a)
    })),
    character(0))
  expect_equal(
    find_vars(quote({
      a <- 1
      f(a, x)
    })),
    "x")
  expect_setequal(
    find_vars(quote({
      a <- a + 1
      f(a, x)
    })),
    c("a", "x"))
})


test_that("can create a bulk submission from an expression", {
  obj <- test_rrq()
  d <- data.frame(x = 1:3, y = c("x", "y", "z"))
  ids <- rrq_task_create_bulk_expr(list(x, y), d, controller = obj)
  expect_length(ids, 3)
  dat1 <- rrq_task_data(ids[[1]], controller = obj)
  expect_equal(dat1$variables, list(x = 1, y = "x"))
  dat3 <- rrq_task_data(ids[[3]], controller = obj)
  expect_equal(dat3$variables, list(x = 3, y = "z"))

  w <- test_worker_blocking(obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(ids[[1]], controller = obj),
               list(1, "x"))
})


test_that("can create a bulk submission from an expression, binding from env", {
  obj <- test_rrq()
  d <- data.frame(x = 1:3, y = c("x", "y", "z"))
  a <- 10
  ids <- rrq_task_create_bulk_expr(list(a, x, y), d, controller = obj)
  expect_length(ids, 3)
  dat1 <- rrq_task_data(ids[[1]], controller = obj)
  dat3 <- rrq_task_data(ids[[3]], controller = obj)
  expect_equal(dat1$variables, list(a = 10, x = 1, y = "x"))
  expect_equal(dat3$variables, list(a = 10, x = 3, y = "z"))
  expect_equal(dat1$variable_in_store, c(TRUE, FALSE, FALSE))

  w <- test_worker_blocking(obj)
  w$step(TRUE)
  expect_equal(rrq_task_result(ids[[1]], controller = obj),
               list(10, 1, "x"))
})


test_that("can create a bulk submission from a call and vector", {
  obj <- test_rrq()
  d <- 1:3
  ids <- rrq_task_create_bulk_call(sqrt, d, controller = obj)
  expect_length(ids, 3)
  dat1 <- rrq_task_data(ids[[1]], controller = obj)
  dat3 <- rrq_task_data(ids[[3]], controller = obj)
  expect_equal(dat1$fn, list(name = "sqrt", namespace = NULL, value = sqrt))
  expect_equal(dat1$args, list(1))
  expect_equal(dat3$fn, dat1$fn)
  expect_equal(dat3$args, list(3))

  w <- test_worker_blocking(obj)
  w$step(TRUE)
  w$step(TRUE)
  w$step(TRUE)
  expect_equal(rrq_task_result(ids[[1]], controller = obj), 1)
  expect_equal(rrq_task_result(ids[[2]], controller = obj), sqrt(2))
  expect_equal(rrq_task_result(ids[[3]], controller = obj), sqrt(3))
})


test_that("can create a bulk submission from a call and data.frame", {
  obj <- test_rrq()
  d <- data.frame(x = 1:3, y = 4:6)
  ids <- rrq_task_create_bulk_call(function(x, y, z) (x + y) / z,
                                   d, args = list(z = 3),
                                   controller = obj)
  expect_length(ids, 3)
  dat1 <- rrq_task_data(ids[[1]], controller = obj)
  dat3 <- rrq_task_data(ids[[3]], controller = obj)
  expect_equal(dat1$args, list(x = 1, y = 4, z = 3))
  expect_equal(dat3$args, list(x = 3, y = 6, z = 3))

  w <- test_worker_blocking(obj)
  w$step(TRUE)
  w$step(TRUE)
  w$step(TRUE)
  expect_equal(rrq_task_result(ids[[1]], controller = obj), 5 / 3)
  expect_equal(rrq_task_result(ids[[2]], controller = obj), 7 / 3)
  expect_equal(rrq_task_result(ids[[3]], controller = obj), 9 / 3)
})


test_that("bulk calls require at least on data element", {
  obj <- test_rrq()
  d <- data.frame(x = numeric(0))
  expect_error(
    rrq_task_create_bulk_call(identity, d$x, controller = obj),
    "'data' must have at least one element")
  expect_error(
    rrq_task_create_bulk_call(identity, d, controller = obj),
    "'data' must have at least one row")
  expect_error(
    rrq_task_create_bulk_expr(identity(x), d, controller = obj),
    "'data' must have at least one row")
})


test_that("bulk expr requires a data.frame", {
  obj <- test_rrq()
  expect_error(
    rrq_task_create_bulk_expr(identity(x), 1:5, controller = obj),
    "Expected 'data' to be a data.frame")
})
