rrq_removed_method <- function(name, replacement = NULL) {
  if (is.null(replacement)) {
    hint <- NULL
  } else {
    hint <- c(i = "Use {replacement} instead")
  }
  cli::cli_abort(c("The '${name}' method has been removed", hint),
                 call = NULL)
}
