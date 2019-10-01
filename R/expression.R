expression_eval_safely <- function(expr, envir) {
  warnings <- collector()
  trace <- collector()

  handler <- function(e) {
    e$trace <- trace$get()
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
       success = !inherits(value, "rrq_task_error"),
       warnings = warnings$get())
}
