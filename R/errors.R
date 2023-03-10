rrq_task_error_group <- function(errs, group_size) {
  ## Could do even more fun things here with summarising over the
  ## error messages (e.g., n threw error x), but this is fine for now.
  msgs <- vcapply(errs, conditionMessage)
  if (length(msgs) > 5) {
    msgs <- c(msgs[1:4], "...")
  }
  msg <- paste(c(sprintf("%d/%d tasks failed:", length(errs), group_size),
                 paste("    -", msgs)), collapse = "\n")
  cls <- c("rrq_task_error_group", "rrq_task_error", "error", "condition")
  structure(list(message = msg, errors = errs), class = cls)
}
