expression_eval_safely <- function(expr, envir) {
  warnings <- collector()
  error <- NULL

  handler <- function(e) {
    e$trace <- utils::limitedLabels(sys.calls())
    class(e) <- c("rrq_task_error", class(e))
    error <<- e
    NULL
  }

  value <- tryCatch(
    withCallingHandlers(
      eval(expr, envir),
      warning = function(e) warnings$add(e$message),
      error = function(e) handler(e)),
    error = function(e) error)

  list(value = value,
       success = is.null(error),
       warnings = warnings$get())
}
