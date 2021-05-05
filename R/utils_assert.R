## Type
assert_is <- function(x, what, name = deparse(substitute(x))) {
  if (!inherits(x, what)) {
    stop(sprintf("%s must inherit from %s", name,
                 paste(what, collapse = " / ")), call. = FALSE)
  }
}

assert_character <- function(x, name = deparse(substitute(x))) {
  if (!is.character(x)) {
    stop(sprintf("%s must be character", name), call. = FALSE)
  }
}

assert_logical <- function(x, name = deparse(substitute(x))) {
  if (!is.logical(x)) {
    stop(sprintf("%s must be logical", name), call. = FALSE)
  }
}

assert_numeric <- function(x, name = deparse(substitute(x))) {
  if (!is.numeric(x)) {
    stop(sprintf("'%s' must be a numeric", name), call. = FALSE)
  }
}

assert_character_or_null <- function(x, name = deparse(substitute(x))) {
  if (!is.null(x)) {
    assert_character(x, name)
  }
}

assert_nonmissing <- function(x, name = deparse(substitute(x))) {
  if (any(is.na(x))) {
    stop(sprintf("'%s' must not be NA", name), call. = FALSE)
  }
}

## Length
assert_scalar <- function(x, name = deparse(substitute(x))) {
  if (length(x) != 1) {
    stop(sprintf("%s must be a scalar", name), call. = FALSE)
  }
}

assert_length <- function(x, n, name = deparse(substitute(x))) {
  if (length(x) != n) {
    stop(sprintf("%s must be length %d", name, n), call. = FALSE)
  }
}

## Compound:
assert_scalar_character <- function(x, name = deparse(substitute(x))) {
  assert_scalar(x, name)
  assert_character(x, name)
  assert_nonmissing(x, name)
}

assert_scalar_logical <- function(x, name = deparse(substitute(x))) {
  assert_scalar(x, name)
  assert_logical(x, name)
  assert_nonmissing(x, name)
}

assert_scalar_numeric <- function(x, name = deparse(substitute(x))) {
  assert_scalar(x, name)
  assert_numeric(x, name)
  assert_nonmissing(x, name)
}

assert_integer_like <- function(x, name = deparse(substitute(x))) {
  if (!isTRUE(all.equal(as.integer(x), x))) {
    stop(sprintf("%s must be integer like", name))
  }
}

assert_scalar_integer_like <- function(x, name = deparse(substitute(x))) {
  assert_scalar(x, name)
  assert_integer_like(x, name)
}

assert_scalar_positive_integer <- function(x, zero_ok = FALSE,
                                           name = deparse(substitute(x))) {
  assert_scalar(x, name)
  assert_nonmissing(x, name)
  assert_integer_like(x, name)
  if (x < if (zero_ok) 0 else 1) {
    stop(sprintf("'%s' must be a positive integer", name), call. = FALSE)
  }
}

assert_valid_timeout <- function(x, name = deparse(substitute(x))) {
  assert_scalar_numeric(x, name)
  if (x < 0) {
    stop(sprintf("'%s' must be positive", name))
  }
}


assert_named <- function(x, unique = FALSE, name = deparse(substitute(x))) {
  if (is.null(names(x))) {
    stop(sprintf("'%s' must be named", name), call. = FALSE)
  }
  if (unique && any(duplicated(names(x)))) {
    stop(sprintf("'%s' must have unique names", name), call. = FALSE)
  }
}
