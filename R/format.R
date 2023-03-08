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

#' @export
format.rrq_worker_info <- function(x, ...) {
  print_one <- function(worker_name) {
    worker <- x[[worker_name]]
    c(sprintf("  %s", worker_name),
      sprintf("    rrq_version:   %s", worker$rrq_version),
      sprintf("    platform:      %s", worker$platform),
      sprintf("    running:       %s", worker$running),
      sprintf("    hostname:      %s", worker$hostname),
      sprintf("    username:      %s", worker$username),
      sprintf("    queue:         %s", worker$queue),
      sprintf("    wd:            %s", worker$wd),
      sprintf("    pid:           %d", worker$pid),
      sprintf("    redis_host:    %s", worker$redis_host),
      sprintf("    redis_port:    %s", worker$redis_port),
      sprintf("    heartbeat_key: %s", worker$heartbeat_key))
  }
  text <- lapply(names(x), print_one)
  paste0(c("<rrq_worker_info>", unlist(text)), collapse = "\n")
}

#' @export
print.rrq_worker_info <- function(x, ...) {
  cat(paste0(format(x, ...), collapse = "\n"))
  invisible(x)
}
