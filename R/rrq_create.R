##' Create a task based on an expression.  The expression passed as
##' `expr` will typically be a function call (e.g., `f(x)`).  We will
##' analyse the expression and find all variables that you reference
##' (in the case of `f(x)` this is `x`) and combine this with the
##' function name to run on the worker.  If `x` cannot be found in
##' your calling environment we will error.
##'
##' Alternatively you may provide a multiline statement by using `{}`
##' to surround multiple lines, such as:
##'
##' ```
##' task_create_expr({
##'   x <- runif(1)
##'   f(x)
##' }, ...)
##' ```
##'
##' in this case, we apply a simple heuristic to work out that `x` is
##' locally assigned and should not be saved with the expression.
##'
##' @title Create a task based on an expression
##'
##' @param expr The expression, does not need quoting. See Details.
##'
##' @param queue The queue to add the task to; if not specified the
##'   "default" queue (which all workers listen to) will be used. If
##'   you have configured workers to listen to more than one queue you
##'   can specify that here. Be warned that if you push jobs onto a
##'   queue with no worker, it will queue forever.
##'
##' @param separate_process Logical, indicating if the task should be
##'   run in a separate process on the worker (see `$enqueue` for
##'   details).
##'
##' @param timeout_task_run Optionally, a maximum allowed running
##'   time, in seconds (see `$enqueue` for details).
##'
##' @param depends_on Vector or list of IDs of tasks which must have
##'   completed before this job can be run. Once all dependent tasks
##'   have been successfully run, this task will get added to the
##'   queue. If the dependent task fails then this task will be
##'   removed from the queue.
##'
##' @inheritParams rrq_task_list
##'
##' @export
rrq_task_create_expr <- function(expr, queue = NULL,
                                 separate_process = FALSE,
                                 timeout_task_run = NULL,
                                 depends_on = NULL,
                                 controller = NULL) {
  controller <- get_controller(controller)
  verify_dependencies_exist(controller, depends_on)
  expr <- check_expression(rlang::enquo(expr))
  variables <- task_variables(find_vars(expr$value), expr$envir)

  task_id <- ids::random_id()
  if (length(variables) > 0) {
    variables_hash <- controller$store$mset(variables, task_id)
  } else {
    variables_hash <- NULL
  }
  dat <- list(type = "expr", expr = expr$value, variables = variables_hash)
  dat <- list(object_to_bin(dat))

  task_submit2(controller, task_id, dat, queue, separate_process,
               timeout_task_run, depends_on)
}


##' Create a task based on a function call.  This is fairly similar to
##' [callr::r], and forms the basis of [lapply()]-like task
##' submission.  Sending a call may have slightly different semantics
##' than you expect if you send a closure (a function that binds
##' data), and we may change behaviour here until we find a happy set
##' of compromises.  See Details for more on this.  The expression
##' `rrq_task_create_call(f, list(a, b, c))` is similar to
##' `rrq_task_create_expr(f(a, b, c))`, use whichever you prefer.
##'
##' Things are pretty unambiguous when you pass in a function from a
##' package, especially when you refer to that package with its
##' namespace (e.g. `pkg::fn`).
##'
##' If you pass in the name *without a namespace* from a package that
##' you have loaded with `library()` locally but you have not loaded
##' with `library` within your worker environment, we may not do the
##' right thing and you may see your task fail, or find a different
##' function with the same name.
##'
##' If you pass in an anonymous function (e.g., `function(x) x + 1`)
##' we may or may not do the right thing with respect to environment
##' capture.  We never capture the global environment so if your
##' function is a closure that tries to bind a symbol from the global
##' environment it will not work.  Like with `callr::r`, anonymous
##' functions will be easiest to think about where they are fully self
##' contained (i.e., all inputs to the functions come through `args`).
##' If you have bound a *local* environment, we may do slightly
##' better, but semantics here are undefined and subject to change.
##'
##' R does some fancy things with function calls that we don't try to
##' replicate.  In particular you may have noticed that this works:
##'
##' ```
##' c <- "x"
##' c(c, c) # a vector of two "x"'s
##' ```
##'
##' You can end up in this situation locally with:
##'
##' ```
##' f <- function(x) x + 1
##' local({
##'   f <- 1
##'   f(f) # 2
##' })
##' ```
##'
##' this is because when R looks for the symbol for the call it skips
##' over non-function objects.  We don't reconstruct environment
##' chains in exactly the same way as you would have locally so this
##' is not possible.
##'
##' @title Create a task from a call
##'
##' @param fn The function to call
##'
##' @param call A list of arguments to pass to the function
##'
##' @inheritParams rrq_task_create_expr
##'
##' @return A task identifier (a 32 character hex string) that you can
##'   pass in to other rrq functions, notably [rrq_task_status()] and
##'   [rrq_task_result()]
##'
##' @export
rrq_task_create_call <- function(fn, args, queue = NULL,
                                 separate_process = FALSE,
                                 timeout_task_run = NULL,
                                 depends_on = NULL,
                                 controller = NULL) {
  controller <- get_controller(controller)
  verify_dependencies_exist(controller, depends_on)
  fn <- check_function(rlang::enquo(fn), rlang::current_env())
  args <- check_args(args)
  task_id <- ids::random_id()

  store <- controller$store
  ## Using suppressWarnings here to avoid a namespace warning that the
  ## user cannot do anything about.
  fn_hash <- suppressWarnings(store$set(fn, task_id))
  args_hash <- set_names(store$mset(args, task_id), names(args))
  dat <- list(type = "call", fn = fn_hash, args = args_hash)
  dat <- list(object_to_bin(dat))

  task_submit2(controller, task_id, dat, queue, separate_process,
               timeout_task_run, depends_on)
}


task_submit2 <- function(controller, task_ids, dat, queue, separate_process,
                         timeout_task_run, depends_on) {
  con <- controller$con
  keys <- controller$keys
  store <- controller$store
  key_complete <- NULL
  task_submit_n(con, keys, store, task_ids, dat, key_complete, queue,
                separate_process, timeout_task_run, depends_on)
}


## This set of functions essentially copied from hipercow
check_expression <- function(quo) {
  if (rlang::quo_is_symbol(quo)) {
    sym <- rlang::as_name(rlang::quo_get_expr(quo))
    envir <- rlang::quo_get_env(quo)
    if (!rlang::env_has(envir, sym, inherit = TRUE)) {
      cli::cli_abort("Could not find expression '{sym}'")
    }
    expr <- rlang::env_get(envir, sym, inherit = TRUE)
    if (!rlang::is_call(expr)) {
      cli::cli_abort(c(
        "Expected 'expr' to be a function call",
        i = paste("You passed a symbol '{sym}', but that turned out to be",
                  "an object of type {typeof(expr)} and not a call")))
    }
  } else {
    if (!rlang::quo_is_call(quo)) {
      cli::cli_abort("Expected 'expr' to be a function call")
    }
    envir <- rlang::quo_get_env(quo)
    expr <- rlang::quo_get_expr(quo)
  }

  if (rlang::is_call(expr, "quote")) {
    given <- rlang::expr_deparse(expr)
    alt <- rlang::expr_deparse(expr[[2]])
    cli::cli_abort(
      c("You have an extra layer of quote() around 'expr'",
        i = "You passed '{given}' but probably meant to pass '{alt}'"))
  }
  list(value = expr, envir = envir)
}


task_variables <- function(names, envir) {
  if (length(names) == 0) {
    NULL
  } else {
    rlang::env_get_list(envir, names, inherit = TRUE, last = topenv())
  }
}


check_function <- function(quo, call = NULL) {
  expr <- rlang::quo_get_expr(quo)
  if (rlang::is_call(expr, "::")) {
    value <- NULL
    name <- as.character(expr[[3]])
    namespace <- as.character(expr[[2]])
  } else if (rlang::is_call(expr, ":::")) {
    value <- expr
    name <- NULL
    namespace <- NULL
  } else if (rlang::is_symbol(expr)) {
    envir <- rlang::quo_get_env(quo)
    name <- as.character(expr)
    namespace <- NULL
    value <- rlang::env_get(envir, name, inherit = TRUE)
    if (!rlang::is_function(value)) {
      cli::cli_abort("The symbol '{name}' is not a function")
    }
  } else {
    name <- NULL
    namespace <- NULL
    value <- eval(expr, rlang::quo_get_env(quo))
    if (!rlang::is_function(value)) {
      cli::cli_abort("The value passed is not a function")
    }
  }
  list(name = name, namespace = namespace, value = value)
}


check_args <- function(args, call = NULL) {
  if (is.null(args)) {
    args <- list()
  }
  if (!is.list(args)) {
    cli::cli_abort("Expeced a list for 'args'", arg = "args", call = call)
  }
  args
}


find_vars <- function(expr, exclude = character()) {
  if (rlang::is_call(expr, "{")) {
    ret <- character()
    for (e in as.list(expr[-1])) {
      if (rlang::is_call(e, c("<-", "<<-", "="))) {
        ret <- c(ret, find_vars(e[[3]], exclude))
        exclude <- c(exclude, as.character(e[[2]]))
      } else {
        ret <- c(ret, find_vars(e, exclude))
      }
    }
    ret
  } else {
    setdiff(all.vars(expr), exclude)
  }
}
