assert_is <- function(x, what, name = deparse(substitute(x)), arg = name,
                      call = NULL) {
  if (!inherits(x, what)) {
    expected <- paste(what, collapse = " / ")
    found <- paste(class(x), collapse = " / ")
    cli::cli_abort(
      c("'{name}' must be a '{expected}'",
        i = "'{name}' was a '{found}'"),
      call = call, arg = arg)
  }
}

assert_character <- function(x, name = deparse(substitute(x)), call = NULL) {
  if (!is.character(x)) {
    cli::cli_abort("'{name}' must be a character", arg = name, call = call)
  }
}

assert_logical <- function(x, name = deparse(substitute(x)), call = NULL) {
  if (!is.logical(x)) {
    cli::cli_abort("'{name}' must be a logical", arg = name, call = call)
  }
}

assert_numeric <- function(x, name = deparse(substitute(x)), call = NULL) {
  if (!is.numeric(x)) {
    cli::cli_abort("'{name}' must be a numeric", arg = name, call = call)
  }
}

assert_nonmissing <- function(x, name = deparse(substitute(x)), call = NULL) {
  if (any(is.na(x))) {
    cli::cli_abort("'{name}' must not be NA", arg = name, call = call)
  }
}

## Length
assert_scalar <- function(x, name = deparse(substitute(x)), call = NULL) {
  if (length(x) != 1) {
    cli::cli_abort("'{name}' must be a scalar", arg = name, call = call)
  }
}

## Compound:
assert_scalar_character <- function(x, name = deparse(substitute(x),
                                                      call = NULL)) {
  assert_scalar(x, name, call)
  assert_character(x, name, call)
  assert_nonmissing(x, name, call)
}

assert_scalar_logical <- function(x, name = deparse(substitute(x)),
                                  call = NULL) {
  assert_scalar(x, name, call)
  assert_logical(x, name, call)
  assert_nonmissing(x, name, call)
}

assert_scalar_numeric <- function(x, name = deparse(substitute(x)),
                                  call = NULL) {
  assert_scalar(x, name, call)
  assert_numeric(x, name, call)
  assert_nonmissing(x, name, call)
}

assert_integer_like <- function(x, name = deparse(substitute(x)), call = NULL) {
  if (!rlang::is_integerish(x)) {
    cli::cli_abort("'{name}' must be integer-like", arg = name, call = call)
  }
}

assert_scalar_integer_like <- function(x, name = deparse(substitute(x)),
                                       call = NULL) {
  assert_scalar(x, name, call)
  assert_integer_like(x, name, call)
}

assert_scalar_positive_integer <- function(x, zero_ok = FALSE,
                                           name = deparse(substitute(x)),
                                           call = NULL) {
  assert_scalar(x, name, call)
  assert_nonmissing(x, name, call)
  assert_integer_like(x, name, call)
  if (x < if (zero_ok) 0 else 1) {
    cli::cli_abort("'{name}' must be a positive integer", arg = name,
                   call = call)
  }
}

assert_valid_timeout <- function(x, name = deparse(substitute(x)),
                                 call = NULL) {
  assert_scalar_numeric(x, name, call)
  if (x < 0) {
    cli::cli_abort("{name}' must be positive", arg = name, call = call)
  }
}


assert_named <- function(x, unique = FALSE, name = deparse(substitute(x)),
                         call = NULL) {
  if (is.null(names(x))) {
    cli::cli_abort("'{name}' must be named", arg = name, call = call)
  }
  if (unique && any(duplicated(names(x)))) {
    cli::cli_abort("'{name}' must have unique names", arg = name, call = call)
  }
}


## This really is a different issue, and should be moved into the real code
assert_file_exists <- function(x, name = "File") {
  err <- !file.exists(x)
  if (any(err)) {
    msg <- paste(squote(x[err]), collapse = ", ")
    stop(sprintf("%s does not exist: %s", name, msg), call. = FALSE)
  }
}
