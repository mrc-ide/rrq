context("redis utilities")

test_that("scan expire", {
  con <- test_hiredis()

  prefix1 <- sprintf("rrq:%s", ids::random_id())
  prefix2 <- sprintf("rrq:%s", ids::random_id())
  keys1 <- sprintf("%s:%s", prefix1, letters)
  keys2 <- sprintf("%s:%s", prefix2, letters)
  pat1 <- sprintf("%s:*", prefix1)
  pat2 <- sprintf("%s:*", prefix2)

  con$MSET(keys1, letters)
  con$MSET(keys2, letters)

  expect_equal(scan_expire(con, pat1, 100), 26)
  expect_gt(con$TTL(keys1[[1]]), 0)
  expect_lte(con$TTL(keys1[[1]]), 100)

  expect_equal(scan_expire(con, pat2, 1), 26)
  Sys.sleep(1.5)
  expect_null(con$GET(keys2[[1]]))

  con$DEL(keys1)
})


test_that("blpop (immediate)", {
  con <- test_hiredis()

  keys <- sprintf("rrq:%s:%d", ids::random_id(), 1:3)
  on.exit(con$DEL(keys))

  expect_null(blpop(con, keys, 0, TRUE))

  con$RPUSH(keys[[1]], "1")
  con$RPUSH(keys[[2]], "2-1")
  con$RPUSH(keys[[2]], "2-2")
  con$RPUSH(keys[[3]], "3")

  expect_equal(blpop(con, keys, 0, TRUE),
               list(keys[[1]], "1"))
  expect_equal(blpop(con, keys, 0, TRUE),
               list(keys[[2]], "2-1"))
  expect_equal(blpop(con, keys, 0, TRUE),
               list(keys[[2]], "2-2"))
  expect_equal(blpop(con, keys, 0, TRUE),
               list(keys[[3]], "3"))
  expect_null(blpop(con, keys, 0, TRUE))
})


test_that("delete", {
  con <- test_hiredis()

  prefix1 <- sprintf("rrq:%s", ids::random_id())
  prefix2 <- sprintf("rrq:%s", ids::random_id())
  keys1 <- sprintf("%s:%s", prefix1, letters)
  keys2 <- sprintf("%s:%s", prefix2, letters)
  pat1 <- sprintf("%s:*", prefix1)
  pat2 <- sprintf("%s:*", prefix2)

  con$MSET(keys1, letters)
  con$MSET(keys2, letters)

  expect_error(
    delete_keys(con, pat1, -1), "Invalid value for delete")
  expect_error(
    delete_keys(con, pat1, NA_integer_), "Invalid value for delete")

  expect_equal(delete_keys(con, pat1, TRUE), 26)
  expect_equal(con$EXISTS(keys1[[1]]), 0)

  expect_equal(delete_keys(con, pat2, 1), 26)
  expect_equal(con$EXISTS(keys2[[1]]), 1)
  Sys.sleep(1)
  expect_equal(con$EXISTS(keys2[[1]]), 0)
})


test_that("push max length", {
  con <- test_hiredis()

  key <- sprintf("rrq:%s", ids::random_id())
  con$RPUSH(key, c(1, 2, 3, 4))
  rpush_max_length(con, key, 5, 5)
  expect_equal(con$LRANGE(key, 0, -1), as.list(as.character(1:5)))
  rpush_max_length(con, key, 6, 5)
  expect_equal(con$LRANGE(key, 0, -1), as.list(as.character(2:6)))
})


test_that("hash_exists", {
  con <- test_hiredis()

  key <- sprintf("rrq:%s", ids::random_id(3))
  field <- c("a", "b", "c", "d")

  expect_equal(hash_exists(con, character(0), character(0)), logical(0))
  expect_equal(hash_exists(con, key, character(0)), logical(0))
  expect_equal(hash_exists(con, character(0), field), logical(0))
  expect_equal(hash_exists(con, key[[1]], field, TRUE), rep(FALSE, 4))
  expect_equal(hash_exists(con, key, field[[1]]), rep(FALSE, 3))

  con$HMSET(key[[1]], field[c(2, 4)], c("x", "y"))
  on.exit(con$DEL(key))

  expect_equal(hash_exists(con, key[[1]], field, TRUE),
               c(FALSE, TRUE, FALSE, TRUE))
  expect_equal(hash_exists(con, key, field[[2]]),
               c(TRUE, FALSE, FALSE))
})
