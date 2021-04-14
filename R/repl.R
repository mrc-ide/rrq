repl_eval <- function(expr, envir, path) {
  withr::with_message_sink(path, {
    withr::with_output_sink(path, {
      expression_eval_safely2(expr, envir)
    })
  })
}


with_autoprint <- function(expr) {
  ans <- withVisible(force(expr))
  if (ans$visible) {
    print(ans$value)
  }
  ans$value
}


expression_eval_safely2 <- function(expr, envir) {
  ## NOTE: basically the a generalised form of expression_eval_safely;
  ## we could build that function easily from this one.
  warnings <- collector(list())
  trace <- collector()


  handler <- function(e) {
    error_target <- list(quote(expression_eval_safely2), quote(eval))
    list(success = FALSE,
         error = e,
         ## This does a good job of lopping off the leading mess, but
         ## we have 1-2 calls at the tail of the stack that are not
         ## here normally due to calling handlers. These vary based on
         ## (at least) if we throw a simple or custom error.
         trace = filter_trace(trace$get(), error_target),
         warnings = warnings$get())
  }

  tryCatch(
    withCallingHandlers(
      list(success = TRUE,
           value = with_autoprint(eval(expr, envir)),
           warnings = warnings$get()),
      warning = function(e) warnings$add(e$message),
      error = function(e) trace$add(sys.calls())),
    error = handler)
}


filter_trace <- function(trace, target) {
  if (length(target) == 0) {
    return(trace)
  }
  t <- target[[1]]
  for (i in seq_along(trace)) {
    x <- trace[[i]]
    if (is.recursive(x) && identical(x[[1]], t)) {
      return(filter_trace(trace[-seq_len(i)], target[-1]))
    }
  }
  trace
}
