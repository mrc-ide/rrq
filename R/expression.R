expression_eval_safely <- function(expr, envir) {
  top <- rlang::current_env()
  top$success <- TRUE
  warnings <- collector()

  handler <- function(e) {
    e$trace <- top$trace
    w <- warnings$get()
    if (length(w) > 0) {
      e$warnings <- w
    }
    e
  }

  value <- tryCatch(
    withCallingHandlers(
      eval(expr, envir),
      warning = function(e) warnings$add(e$message),
      error = function(e) {
        top$success <- FALSE
        top$trace <- rlang::trace_back(top)
      }),
    error = handler)

  list(value = value, success = top$success)
}


## Preparing an expression for remote evaluation.  There are lots of
## ways that this can be done really.
##
## In context we try to set up an environment that is replicated
## elsewhere and that gives an environment that we can query for the
## existance of values.  We analyse the expression to work out what
## might be needed, looking for symbols because we want to evaluate
## the actual expression.
##
## The future package takes a similar approach - analyse an expression
## and work out what is likely to be present.  It allows specification
## of a "globals" argument to fine tune this.0
##
## This version is fairy basic:
##
## * accept a list of objects to export (or their names), or
## * automatically analyse an expression
##
## The only complication is if the function value also needs exporting
## (e.g., in the case of an anonymous function).
##
## This function was derived from context::prepare_expression
expression_prepare <- function(expr, envir_call, store, tag,
                               function_value = NULL, export = NULL) {
  ret <- list(expr = expr)

  if (!is.null(function_value)) {
    assert_is(function_value, "function")
    hash <- store$set(function_value, tag)
    ret$function_hash <- hash
    ret$expr[[1L]] <- as.name(hash)
  }

  if (is.null(export)) {
    symbols <- expression_find_symbols(expr)
    if (length(symbols) > 0L) {
      is_local <- vlapply(symbols, exists, envir_call, inherits = FALSE,
                          USE.NAMES = FALSE)

      if (any(is_local)) {
        collect <- symbols[is_local]
        export <- set_names(
          lapply(collect, get, envir_call, inherits = FALSE),
          collect)
      }
    }
  } else {
    if (is.character(export)) {
      export <- set_names(lapply(export, get, envir_call, inherits = FALSE),
                          export)
    } else {
      assert_is(export, "list")
      assert_named(export, TRUE)
    }
  }

  if (length(export) > 0L) {
    h <- store$mset(export, tag)
    ret$objects <- set_names(h, names(export))
  }

  ret
}


expression_find_symbols <- function(expr) {
  symbols <- collector()
  namespace <- quote(`::`)
  hidden <- quote(`:::`)
  dollar <- quote(`$`)
  stop_at <- c(namespace, hidden, dollar)

  descend <- function(e) {
    if (!is.recursive(e)) {
      if (is.symbol(e)) {
        symbols$add(deparse(e))
      }
    } else if (!is_call(e, stop_at)) {
      for (a in as.list(e)) {
        if (!missing(a)) {
          descend(a)
        }
      }
    }
  }

  if (!is.call(expr) || identical(expr[[1]], quote(`::`))) {
    stop("Expected a call")
  }

  descend(expr[-1L])
  unique(symbols$get())
}


expression_restore_locals <- function(dat, parent, store) {
  e <- new.env(parent = parent)
  objects <- dat$objects
  if (!is.null(dat$function_hash)) {
    objects <- c(objects, set_names(dat$function_hash, dat$function_hash))
  }
  if (length(objects) > 0L) {
    list2env(set_names(store$mget(objects), names(objects)), e)
  }
  e
}
