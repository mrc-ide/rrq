expression_eval_safely <- function(expr, envir) {
  warnings <- collector()
  trace <- collector()

  handler <- function(e) {
    e$trace <- trace$get()
    w <- warnings$get()
    if (length(w) > 0) {
      e$warnings <- w
    }
    class(e) <- c("rrq_task_error", class(e))
    e
  }

  value <- tryCatch(
    withCallingHandlers(
      eval(expr, envir),
      warning = function(e) warnings$add(e$message),
      error = function(e) trace$add(utils::limitedLabels(sys.calls()))),
    error = handler)

  list(value = value,
       success = !inherits(value, "rrq_task_error"))
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
## of a "globals" argument to fine tune this.
##
## For a nice mix of flexibility here, we should offer a few modes I think:
##
## * do no analysis of the expression
## * export an explicit list of variables to the environment
## * ignore a specific list of variables
## * automatically analyse an expression
##
## This function was derived from context::prepare_expression
##
## Once this works we will split it apart for use within the call()
## and then bulk functions too.
expression_prepare <- function(expr, envir_call, envir_base, db,
                               analyse = TRUE, include = NULL, exclude = NULL,
                               function_value = NULL) {
  ret <- list(expr = expr)

  if (!is.null(function_value)) {
    assert_is(function_value, "function")
    hash <- db$set_by_value(function_value, "objects")
    ret$function_hash <- hash
    ret$expr[[1L]] <- as.name(hash)
  }

  objects <- list()
  if (analyse) {
    symbols <- expression_find_symbols(expr)
    if (length(symbols) > 0L) {
      is_local <- vlapply(symbols, exists, envir_call, inherits = FALSE,
                          USE.NAMES = FALSE)

      if (!is.null(envir_base) && any(!is_local)) {
        test <- symbols[!is_local]
        is_in_base <- vlapply(test, exists, envir_base, USE.NAMES = FALSE)
        if (any(!is_in_base)) {
          stop("not all objects found: ",
               paste(test[!is_in_base], collapse = ", "))
        }
      }

      if (any(is_local)) {
        collect <- symbols[is_local]
        objects <- set_names(
          lapply(collect, get, envir_call, inherits = FALSE),
          collect)
      }
    }
  }

  if (length(include) > 0L) {
    msg <- setdiff(include, names(objects))
    objects[msg] <- lapply(msg, get, envir_call, inherits = TRUE)
  }

  if (length(exclude) > 0L) {
    objects <- objects[!(names(objects) %in% exclude)]
  }

  if (length(objects) > 0L) {
    h <- db$mset_by_value(objects, "objects")
    ret$objects <- set_names(h, names(objects))
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


expression_restore_locals <- function(dat, parent, db) {
  e <- new.env(parent = parent)
  objects <- dat$objects
  if (!is.null(dat$function_hash)) {
    objects <- c(objects, set_names(dat$function_hash, dat$function_hash))
  }
  if (length(objects) > 0L) {
    db$export(e, objects, "objects")
  }
  e
}
