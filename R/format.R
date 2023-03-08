##' @export
format.rrq_task_error <- function(x, width = 180, ...) {
  call <- conditionCall(x)
  c("<rrq_task_error>",
    if (!is.null(call)) {
      sprintf("  from:   %s", deparse1(call, width.cutoff = width))
    },
    sprintf("  error:  %s", conditionMessage(x)),
    sprintf("  queue:  %s", x$queue_id),
    sprintf("  task:   %s", x$task_id),
    sprintf("  status: %s", x$status),
    "  * To throw this error, use stop() with it",
    if (!is.null(x$trace)) {
      "  * This error has a stack trace, use '$trace' to see it"
    },
    if (!is.null(x$warnings)) {
      "  * This error has warnings, use '$warnings' to see them"
    })
}


##' @export
print.rrq_task_error <- function(x, ...) {
  cat(paste0(format(x, ...), "\n", collapse = ""))
  invisible(x)
}
