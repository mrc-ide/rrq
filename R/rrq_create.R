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
##'   run in a separate process on the worker. If `TRUE`, then the
##'   worker runs the task in a separate process using the `callr`
##'   package. This means that the worker environment is completely
##'   clean, subsequent runs are not affected by preceding ones.  The
##'   downside of this approach is a considerable overhead in starting
##'   the external process and transferring data back.
##'
##' @param timeout_task_run Optionally, a maximum allowed running
##'   time, in seconds. This parameter only has an effect if
##'   `separate_process` is `TRUE`. If given, then if the task takes
##'   longer than this time it will be stopped and the task status set
##'   to `TIMEOUT`.
##'
##' @param depends_on Vector or list of IDs of tasks which must have
##'   completed before this job can be run. Once all dependent tasks
##'   have been successfully run, this task will get added to the
##'   queue. If the dependent task fails then this task will be
##'   removed from the queue.
##'
##' @inheritParams rrq_task_list
##'
##' @seealso [rrq_task_create_call] for creating a task from a
##'   function and arguments to the function, and
##'   [rrq_task_create_bulk_expr] for creating many tasks from a call
##'   and a data.frame
##'
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' # Simple use of the function to create a task based on a function call
##' t <- rrq_task_create_expr(sqrt(2), controller = obj)
##' rrq_task_wait(t, controller = obj)
##' rrq_task_result(t, controller = obj)
##'
##' # The expression can contain calls to other variables, and these
##' # will be included in the call:
##' a <- 3
##' t <- rrq_task_create_expr(sqrt(a), controller = obj)
##' rrq_task_wait(t, controller = obj)
##' rrq_task_result(t, controller = obj)
##'
##' # You can pass in an expression _as_ a symbol too:
##' expr <- quote(sqrt(4))
##' t <- rrq_task_create_expr(expr, controller = obj)
##' rrq_task_wait(t, controller = obj)
##' rrq_task_result(t, controller = obj)
##'
##' # If you queue tasks into separate processes you can use a timeout
##' # to kill the task if it takes too long:
##' t <- rrq_task_create_expr(Sys.sleep(3),
##'                           separate_process = TRUE,
##'                           timeout_task_run = 1,
##'                           controller = obj)
##' rrq_task_wait(t, controller = obj)
##' rrq_task_result(t, controller = obj)
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

  task_submit(controller, task_id, dat, queue, separate_process,
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
##' @param args A list of arguments to pass to the function
##'
##' @inheritParams rrq_task_create_expr
##'
##' @return A task identifier (a 32 character hex string) that you can
##'   pass in to other rrq functions, notably [rrq_task_status()] and
##'   [rrq_task_result()]
##'
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##' t <- rrq_task_create_call(sqrt, list(2), controller = obj)
##' rrq_task_wait(t, controller = obj)
##' rrq_task_result(t, controller = obj)
rrq_task_create_call <- function(fn, args, queue = NULL,
                                 separate_process = FALSE,
                                 timeout_task_run = NULL,
                                 depends_on = NULL,
                                 controller = NULL) {
  controller <- get_controller(controller)
  verify_dependencies_exist(controller, depends_on)
  fn <- check_function(rlang::enquo(fn), call = rlang::current_env())
  args <- check_args(args)
  task_id <- ids::random_id()

  store <- controller$store
  ## Using suppressWarnings here to avoid a namespace warning that the
  ## user cannot do anything about.
  fn_hash <- suppressWarnings(store$set(fn, task_id))
  args_hash <- set_names(store$mset(args, task_id), names(args))
  dat <- list(type = "call", fn = fn_hash, args = args_hash)
  dat <- list(object_to_bin(dat))

  task_submit(controller, task_id, dat, queue, separate_process,
              timeout_task_run, depends_on)
}


##' Create a bulk set of tasks. Variables in `data` take precedence
##' over variables in the environment in which `expr` was
##' created. There is no "pronoun" support yet (see rlang docs).  Use
##' `!!` to pull a variable from the environment if you need to, but
##' be careful not to inject something really large (e.g., any vector
##' really) or you'll end up with a revolting expression and poor
##' backtraces.
##'
##' @title Create bulk tasks from an expression
##'
##' @param expr An expression, as for [rrq_task_create_expr]
##'
##' @param data Data that you wish to inject _row-wise_ into the expression
##'
##' @inheritParams rrq_task_create_expr
##'
##' @return A character vector with task identifiers; this will have a
##'   length equal to the number of row in `data`
##'
##' @export
##'
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' # Create 10 tasks:
##' ts <- rrq_task_create_bulk_expr(sqrt(x), data.frame(x = 1:10),
##'                                 controller = obj)
##' rrq_task_wait(ts, controller = obj)
##' rrq_task_results(ts, controller = obj)
##'
##' # Note that there is no automatic simplification when fetching
##' # results, you might use unlist or vapply to turn this into a
##' # numeric vector rather than a list
##'
##' # The data.frame substituted in may have multiple columns
##' # representing multiple variables to substitute into the
##' # expression
##' d <- expand.grid(a = 1:4, b = 1:4)
##' ts <- rrq_task_create_bulk_expr(a * b, d, controller = obj)
##' rrq_task_wait(ts, controller = obj)
##' rrq_task_results(ts, controller = obj)
rrq_task_create_bulk_expr <- function(expr, data, queue = NULL,
                                      separate_process = FALSE,
                                      timeout_task_run = NULL,
                                      depends_on = NULL,
                                      controller = NULL) {
  controller <- get_controller(controller)
  verify_dependencies_exist(controller, depends_on)
  if (!inherits(data, "data.frame")) {
    cli::cli_abort("Expected 'data' to be a data.frame (or tbl, etc)",
                   arg = "data")
  }
  nr <- nrow(data)
  nc <- ncol(data)
  if (nr == 0) {
    cli::cli_abort("'data' must have at least one row", arg = "data")
  }

  ## This will allow `!!x` to reference a value in the enclosing
  ## environment and we'll splice it into the expression. This will
  ## work pretty well for simple things and _terribly_ for large
  ## objects, which would be better pulled in by name if possible.
  ##
  ## We could do this using "eval_tidy" and use "pronouns" but that
  ## will require a little more setup; probably worth considering
  ## though.  For now this is fine, but we can improve this by:
  ##
  ## * Not doing the injection until later
  ## * Setting up the bits for eval_tidy and exporting them
  ## * Analysing the expression before injection and making sure
  ##   that anything injected is small
  expr <- check_expression(rlang::inject(rlang::enquo(expr)))

  task_ids <- ids::random_id(nr)

  extra <- setdiff(find_vars(expr$value), names(data))
  variables <- task_variables(extra, expr$envir)
  if (length(variables) > 0) {
    variables_hash <- controller$store$mset(variables, task_ids)
  } else {
    variables_hash <- NULL
  }

  variable_in_store <- rep(c(TRUE, FALSE), c(length(variables_hash), nc))

  data <- df_rows(data)
  dat <- lapply(seq_len(nr), function(i) {
    variables_i <- c(variables_hash, data[[i]])
    dat <- list(type = "expr", expr = expr$value, variables = variables_i,
                variable_in_store = variable_in_store)
    object_to_bin(dat)
  })

  task_submit(controller, task_ids, dat, queue, separate_process,
              timeout_task_run, depends_on)
}


##' Create a bulk set of tasks based on applying a function over a
##' vector or [data.frame].  This is the bulk equivalent of
##' [rrq_task_create_call], in the same way that
##' [rrq_task_create_bulk_expr] is a bulk version of
##' [rrq_task_create_expr].
##'
##' @title Create bulk tasks from a call
##'
##' @param fn The function to call
##'
##' @param data The data to apply the function over.  This can be a
##'   vector or list, in which case we act like `lapply` and apply
##'   `fn` to each element in turn.  Alternatively, this can be a
##'   [data.frame], in which case each row is taken as a set of
##'   arguments to `fn`.  Note that if `data` is a `data.frame` then
##'   all arguments to `fn` are named.
##'
##' @param args Additional arguments to `fn`, shared across all calls.
##'   These must be named.  If you are using a `data.frame` for
##'   `data`, you'd probably be better off adding additional columns
##'   that don't vary across rows, but the end result is the same.
##'
##' @inheritParams rrq_task_create_call
##'
##' @return A vector of task identfiers; this will have the length as
##'   `data` has rows if it is a `data.frame`, otherwise it has the
##'   same length as `data`
##'
##' @export
##' @examplesIf rrq:::enable_examples(require_queue = "rrq:example")
##' obj <- rrq_controller("rrq:example")
##'
##' d <- data.frame(n = 1:10, lambda = rgamma(10, 5))
##' ts <- rrq_task_create_bulk_call(rpois, d, controller = obj)
##' rrq_task_wait(ts, controller = obj)
##' rrq_task_results(ts, controller = obj)
rrq_task_create_bulk_call <- function(fn, data, args = NULL,
                                      queue = NULL,
                                      separate_process = FALSE,
                                      timeout_task_run = NULL,
                                      depends_on = NULL,
                                      controller = NULL) {
  controller <- get_controller(controller)
  verify_dependencies_exist(controller, depends_on)
  fn <- check_function(rlang::enquo(fn), call = rlang::current_env())
  if (!is.null(args)) {
    args <- check_args(args)
  }

  is_data_frame <- inherits(data, "data.frame")
  if (is_data_frame) {
    nc <- ncol(data)
    nr <- nrow(data)
    data <- df_rows(data)
  } else {
    nc <- 1
    nr <- length(data)
    data <- lapply(data, function(x) list(x))
  }
  if (nr == 0) {
    cli::cli_abort(
      "'data' must have at least one {if (is_data_frame) 'row' else 'element'}",
      arg = "data")
  }

  store <- controller$store
  task_ids <- ids::random_id(nr)

  ## Using suppressWarnings here to avoid a namespace warning that the
  ## user cannot do anything about.
  fn_hash <- suppressWarnings(store$set(fn, task_ids))
  if (is.null(args)) {
    args_hash <- NULL
  } else {
    args_hash <- set_names(store$mset(args, task_ids), names(args))
  }
  arg_in_store <- rep(c(FALSE, TRUE), c(nc, length(args)))

  dat <- lapply(seq_len(nr), function(i) {
    args_i <- c(data[[i]], args_hash)
    dat <- list(type = "call", fn = fn_hash, args = args_i,
                arg_in_store = arg_in_store)
    object_to_bin(dat)
  })

  task_submit(controller, task_ids, dat, queue, separate_process,
              timeout_task_run, depends_on)
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
    rlang::env_get_list(envir, names, inherit = TRUE)
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
