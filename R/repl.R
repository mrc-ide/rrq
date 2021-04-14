## Once working, all usage of cat(...) can safely become
## message(..., appendLF = FALSE) because the handling will dump that all
## into one stream.
repl_eval <- function(expr, envir, path) {
  withr::with_message_sink(path, {
    withr::with_output_sink(path, {
      res <- repl_eval_expression(expr, envir)
      if (res$success) {
        if (res$value$visible) {
          print(res$value$value)
        }
      } else {
        cat(as.character(res$error))
      }
      show_warnings(res$warnings, res$success)
      res
    })
  })
}


## The next step here is getting the error/warning reporting working correctly.
repl_eval_expression <- function(expr, envir) {
  warnings <- collector(list())
  trace <- collector(list())
  stack_skip <- length(sys.calls()) + 7L

  handle_error <- function(e) {
    error_target <- list(quote(repl_eval_expression), quote(eval))
    list(success = FALSE,
         error = e,
         ## This does a good job of lopping off the leading mess, but
         ## we have 1-2 calls at the tail of the stack that are not
         ## here normally due to calling handlers. These vary based on
         ## (at least) if we throw a simple or custom error.
         ##
         ## I am not completely sure that we can even rely on stop()
         ## being in the trace either.
         trace = trace$get()[-seq_len(stack_skip)],
         warnings = warnings$get())
  }

  handle_warning <- function(e) {
    ## Only collect warnings when option
    if (getOption("warn") == 0) {
      warnings$add(e$message)
    }
    ## But prevent them bubbling up to the top level at the moment
    tryInvokeRestart("muffleWarning")
  }

  tryCatch(
    withCallingHandlers(
      list(success = TRUE,
           value = withVisible(eval(expr, envir)),
           warnings = warnings$get()),
      warning = handle_warning,
      error = function(e) trace$add(sys.calls())),
    error = handle_error)
}


## This is generally pretty nasty, and I wonder if it's even
## worthwhile given how awful warnings are. We could just recommend
## that users set options(warn = 1) to make them become visible as
## they appear.
show_warnings <- function(warnings, success) {
  n <- length(warnings)
  nmax <- getOption("nwarnings")
  if (n == 0) {
    return()
  }

  if (!success) {
    message("In addition: ", appendLF = FALSE)
  }

  if (n <= 10) {
    print.warnings(setNames(vector("list", n), warnings))
  } else if (n <= nmax) {
    message(sprintf(ngettext(
      n,
      "There was %d warning (use warnings() to see it)",
      "There were %d warnings (use warnings() to see them)")))
  } else {
    message(sprintf(
      "There were %d or more warnings (use warnings() to see ther first %d)",
      nmax, nmax))
  }
}


repl_log_forwarder <- function(path, key) {
  callr::r_bg(repl_log_forwarder_helper, list(path, key), package = TRUE)
}


repl_log_forwarder_helper <- function(path, key) {
  con <- redux::hiredis()
  ## There is probably a nicer way of doing this, but this does seem
  ## to work. The issue is that we either have to have a blocking
  ## writer (which is problematic for the host process) or the fifo
  ## nees to create first. This little mess creates a fifo, then
  ## starts reading on it.
  if (!file.exists(path)) {
    tryCatch(suppressWarnings(fifo(path, "w")), error = function(e) NULL)
  }
  p <- fifo(path, "r", blocking = TRUE)
  while (file.exists(path)) {
    txt <- readLines(p, 1L)
    if (length(txt) > 0) {
      con$RPUSH(key, txt)
    }
  }
}
