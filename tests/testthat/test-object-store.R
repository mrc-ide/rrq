context("object_store")

test_that("Fast noop operations behave as expected", {
  con <- redux::hiredis()
  prefix <- ids::random_id(1, 4)
  s <- object_store$new(con, prefix)
  on.exit(s$destroy())

  expect_equal(s$mget(character(0)), list())
  expect_equal(s$mset(list(), "tag"), character(0))
  expect_equal(s$list(), character(0))
  expect_null(s$drop(character(0)), character(0))
  expect_null(s$drop(ids::random_id()), character(0))
})


test_that("Full redis-based storage", {
  con <- redux::hiredis()
  prefix <- ids::random_id(1, 4)

  s <- object_store$new(con, prefix)

  t1 <- ids::random_id()
  t2 <- ids::random_id()
  x <- runif(20)
  y <- rnorm(20)
  z <- rexp(20)
  a <- list(1, x, 10, y)
  b <- list(2, y, 1, z)
  h1 <- s$mset(a, t1)
  h2 <- s$mset(b, t2)
  expect_equal(s$mget(h1), a)
  expect_equal(s$mget(h2), b)

  expect_setequal(s$list(), union(h1, h2))
  expect_setequal(
    redux::scan_find(con, sprintf("%s:tag_hash:*", prefix)),
    sprintf("%s:tag_hash:%s", prefix, c(t1, t2)))

  res <- s$drop(t1)

  expect_setequal(s$list(), h2)
  expect_setequal(
    redux::scan_find(con, sprintf("%s:tag_hash:*", prefix)),
    sprintf("%s:tag_hash:%s", prefix, t2))

  expect_error(s$mget(h1), "Some hashes were not found!")
  expect_equal(s$mget(h2), b)

  s$drop(t2)

  expect_setequal(s$list(), character(0))
  expect_setequal(
    redux::scan_find(con, sprintf("%s:tag_hash:*", prefix)),
    character(0))
})


test_that("Can offload storage", {
  con <- redux::hiredis()
  prefix <- ids::random_id(1, 4)
  path <- tempfile()
  offload <- object_store_offload_disk$new(path)
  s <- object_store$new(con, prefix, 100, offload)

  t1 <- ids::random_id()
  t2 <- ids::random_id()
  x <- runif(20)
  y <- rnorm(20)
  z <- rexp(20)
  a <- list(1, x, 10, y)
  b <- list(2, y, 1, z)
  h1 <- s$mset(a, t1)
  h2 <- s$mset(b, t2)
  expect_equal(s$mget(h1), a)
  expect_equal(s$mget(h2), b)

  expect_setequal(s$list(), union(h1, h2))

  expect_setequal(dir(path), c(h1[c(2, 4)], h2[c(2, 4)]))

  res <- s$drop(t1)

  expect_setequal(dir(path), h2[c(2, 4)])

  expect_error(s$mget(h1), "Some hashes were not found!")
  expect_equal(s$mget(h2), b)

  s$drop(t2)

  expect_setequal(s$list(), character(0))
  expect_equal(dir(path), character(0))
  expect_setequal(
    redux::scan_find(con, sprintf("%s:tag_hash:*", prefix)),
    character(0))
})


test_that("Drop multiple tags", {
  con <- redux::hiredis()
  prefix <- ids::random_id(1, 4)
  s <- object_store$new(con, prefix)
  t <- ids::random_id(2)
  s$mset(list(1), t[[1]])
  s$mset(list(2), t[[2]])
  s$drop(t)

  expect_equal(s$list(), character(0))
})


test_that("scalar helper functions return single values", {
  con <- redux::hiredis()
  prefix <- ids::random_id(1, 4)
  s <- object_store$new(con, prefix)
  on.exit(s$destroy())

  t <- ids::random_id()
  h <- s$set(pi, t)
  expect_equal(h, hash_data(object_to_bin(pi)))

  expect_equal(s$get(h), pi)
})


test_that("destroying a store removes everything, including offload", {
  con <- redux::hiredis()
  prefix <- ids::random_id(1, 4)
  path <- tempfile()
  offload <- object_store_offload_disk$new(path)
  s <- object_store$new(con, prefix, 100, offload)

  t <- ids::random_id()
  x <- runif(20)
  h <- s$mset(list(pi, x), t)

  s$destroy()

  expect_equal(dir(path), character(0))
  expect_false(file.exists(path))
})


test_that("prevent use of offload if disabled", {
  con <- redux::hiredis()
  prefix <- ids::random_id(1, 4)
  path <- tempfile()
  offload <- object_store_offload_disk$new(path)
  s1 <- object_store$new(con, prefix, 100, NULL)
  s2 <- object_store$new(con, prefix, 100, offload)

  t <- ids::random_id()
  x <- runif(20)
  expect_error(s1$mset(list(pi, x), t),
               "offload is not supported")
  h <- s2$mset(list(pi, x), t)
  expect_setequal(s2$list(), h)

  expect_equal(s1$list(), h[[1]])

  expect_error(s1$mget(h), "offload is not supported")
  expect_error(s1$drop(t), "offload is not supported")
})
