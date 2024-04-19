##' @title rrq queue controller
##'
##' @description
##' A queue controller.  Use this to interact with a queue/cluster.
##'
##' @section Task lifecycle:
##'
##' * A task is queued with `$enqueue()`, at which point it becomes `PENDING`
##' * Once a worker selects the task to run, it becomes `RUNNING`
##' * If the task completes successfully without error it becomes `COMPLETE`
##' * If the task throws an error, it becomes `ERROR`
##' * If the task was cancelled (e.g., via `$task_cancel()`) it becomes
##'   `CANCELLED`
##' * If the task is killed by an external process, crashes or the worker
##'   dies (and is running a heartbeat) then the task becomes `DIED`.
##' * The status of an unknown task is `MISSING`
##' * Tasks in any terminal state (except `IMPOSSIBLE`) may be retried
##'   with `task_retry` at which point they become `MOVED`, see
##'   `vignette("fault-tolerance")` for details
##'
##' @section Worker lifecycle:
##'
##' * A worker appears and is `IDLE`
##' * When running a task it is `BUSY`
##' * If it receives a `PAUSE` message it becomes `PAUSED` until it
##'   receives a `RESUME` message
##' * If it exits cleanly (e.g., via a `STOP` message or a timeout) it
##'   becomes `EXITED`
##' * If it crashes and was running a heartbeat, it becomes `LOST`
##'
##' @section Messages:
##'
##' Most of the time workers process tasks, but you can also send them
##'   "messages". Messages take priority over tasks, so if a worker
##'   becomes idle (by coming online or by finishing a task) it will
##'   consume all available messages before starting on a new task,
##'   even if both are available.
##'
##' Each message has a "command" and may have "arguments" to that
##'   command. The supported messages are:
##'
##' * `PING` (no args): "ping" the worker, if alive it will respond
##'   with "PONG"
##'
##' * `ECHO` (accepts an argument of a string): Print a string to the
##'   terminal and log of the worker. Will respond with `OK` once the
##'   message has been printed.
##'
##' * `EVAL` (accepts a string or a quoted expression): Evaluate an
##'   arbitrary R expression on the worker. Responds with the value of
##'   this expression.
##'
##' * `STOP` (accepts a string to print as the worker exits, defaults
##'   to "BYE"): Tells the worker to stop.
##'
##' * `INFO` (no args): Returns information about the worker (versions
##'   of packages, hostname, pid, etc).
##'
##' * `PAUSE` (no args): Tells the worker to stop accepting tasks
##'   (until it receives a `RESUME` message). Messages are processed
##'   as normal.
##'
##' * `RESUME` (no args): Tells a paused worker to resume accepting
##'   tasks.
##'
##' * `REFRESH` (no args): Tells the worker to rebuild their
##'   environment with the `create` method.
##'
##' * `TIMEOUT_SET` (accepts a number, representing seconds): Updates
##'   the worker timeout - the length of time after which it will exit
##'   if it has not processed a task.
##'
##' * `TIMEOUT_GET` (no args): Tells the worker to respond with its
##'   current timeout.
##'
##' @param follow Optional logical, indicating if we should follow any
##'   redirects set up by doing `$task_retry`. If not given, falls
##'   back on the value passed into the queue, the global option
##'   `rrq.follow`, and finally `TRUE`. Set to `FALSE` if you want to
##'   return information about the original task, even if it has been
##'   subsequently retried.
##'
##' @export
rrq_controller <- R6::R6Class(
  "rrq_controller",
  cloneable = FALSE,

  public = list(
    ##' @field con The redis connection. This is part of the
    ##' public API and can be used to access the same redis database
    ##' as the queue.
    con = NULL,

    ##' @field queue_id The queue id used on creation. This is read-only
    ##' after creation.
    queue_id = NULL,

    ##' @description Constructor
    ##' @param queue_id An identifier for the queue.  This will prefix all
    ##'   keys in redis, so a prefix might be useful here depending on
    ##'   your use case (e.g. \code{rrq:<user>:<id>})
    ##'
    ##' @param con A redis connection. The default tries to create a redis
    ##'   connection using default ports, or environment variables set as in
    ##'   [redux::hiredis()]
    ##'
    ##' @param timeout_task_wait An optional default timeout to use when
    ##'   waiting for tasks (e.g., with `$task_wait()`, `$tasks_wait()`,
    ##'   `$lapply()`, etc). If not given, then we fall back on the
    ##'   global option `rrq.timeout_task_wait`, and if that is not set,
    ##    we wait forever (i.e., `timeout_task_wait = Inf`).
    ##'
    ##' @param follow An optional default logical to use for tasks
    ##'   that may (or may not) be retried. If not given we fall back
    ##'   on the global option `rrq.follow`, and if that is not set then
    ##'   `TRUE` (i.e., we do follow). The value `follow = TRUE` is
    ##'   potentially slower than `follow = FALSE` for some operations
    ##'   because we need to dereference every task id. If you never use
    ##'   `$task_retry` then this dereference never has an effect and we
    ##'   can skip it. See `vignette("fault-tolerance")` for more
    ##'   information.
    ##'
    ##' @param check_version Check that the schema version is correct
    initialize = function(queue_id, con = redux::hiredis(),
                          timeout_task_wait = NULL, follow = NULL,
                          check_version = TRUE) {
      assert_scalar_character(queue_id)
      assert_is(con, "redis_api")

      self$con <- con
      self$queue_id <- queue_id
      private$keys <- rrq_keys(self$queue_id)

      if (is.null(timeout_task_wait)) {
        private$timeout_task_wait <- getOption("rrq.timeout_task_wait", Inf)
      } else {
        assert_scalar_positive_integer(timeout_task_wait)
        private$timeout_task_wait <- timeout_task_wait
      }

      if (is.null(follow)) {
        private$follow <- getOption("rrq.follow", TRUE)
      } else {
        assert_scalar_logical(follow)
        private$follow <- follow
      }

      if (check_version) {
        rrq_version_check(self$con, private$keys)
      }
      self$worker_config_save(WORKER_CONFIG_DEFAULT, rrq_worker_config(),
                              overwrite = FALSE)

      private$store <- rrq_object_store(self$con, private$keys)
      private$scripts <- rrq_scripts_load(self$con)
      info <- object_to_bin(controller_info())
      rpush_max_length(self$con, private$keys$controller, info, 10)

      lockBinding("queue_id", self)
    },

    ##' @description Convert controller to the new-style object.
    ##'   Please don't use this in packages directly
    to_v2 = function() {
      ret <- list(queue_id = private$keys$queue_id,
                  con = self$con,
                  keys = private$keys,
                  timeout_task_wait = private$timeout_task_wait,
                  follow = private$follow,
                  scripts = private$scripts,
                  store = private$store)
      class(ret) <- "rrq_controller2"
      ret
    },

    ##' @description Register a function to create an environment when
    ##'   creating a worker. When a worker starts, they will run this
    ##'   function.
    ##'
    ##' @param create A function that will create an environment. It will
    ##'   be called with one parameter (an environment), in a fresh R
    ##'   session. The function [rrq::rrq_envir()] can be used to
    ##'   create a suitable function for the most common case (loading
    ##'   packages and sourcing scripts).
    ##'
    ##' @param notify Boolean, indicating if we should send a `REFRESH`
    ##'   message to all workers to update their environment.
    envir = function(create, notify = TRUE) {
      rrq_worker_envir_set(create, notify, self)
    },

    ##' @description Queue an expression
    ##'
    ##' @param expr Any R expression, unevaluated
    ##'
    ##' @param envir The environment that you would run this expression in
    ##'   locally. This will be used to copy across any dependent variables.
    ##'   For example, if your expression is `sum(1 + a)`, we will also send
    ##'   the value of `a` to the worker along with the expression.
    ##
    ##' @param queue The queue to add the task to; if not specified the
    ##'   "default" queue (which all workers listen to) will be
    ##'   used. If you have configured workers to listen to more than
    ##'   one queue you can specify that here. Be warned that if you
    ##'   push jobs onto a queue with no worker, it will queue forever.
    ##'
    ##' @param separate_process Logical, indicating if the task should be
    ##'   run in a separate process on the worker. If `TRUE`, then the
    ##'   worker runs the task in a separate process using the `callr`
    ##'   package. This means that the worker environment is completely
    ##'   clean, subsequent runs are not affected by preceding ones.
    ##'   The downside of this approach is a considerable overhead in
    ##'   starting the external process and transferring data back.
    ##'
    ##' @param timeout_task_run Optionally, a maximum allowed running time, in
    ##'   seconds. This parameter only has an effect if `separate_process`
    ##'   is `TRUE`. If given, then if the task takes longer than this
    ##'   time it will be stopped and the task status set to `TIMEOUT`.
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue.
    ##'
    ##' @param export Optionally a list of variables to export for the
    ##'   calculation. If given then no automatic analysis of the
    ##'   expression is done. It should be either a named list (name
    ##'   being the variable name, value being the value) or a
    ##'   character vector of variables that can be found immediately
    ##'   within `envir`. Use this where you have already done analysis
    ##'   of the expression (e.g., with the future package / globals)
    ##'   or where you want to avoid moving large objects through Redis
    ##'   that will be available on the remote workers due to how you
    ##'   have configured your worker environment.
    enqueue = function(expr, envir = parent.frame(), queue = NULL,
                       separate_process = FALSE, timeout_task_run = NULL,
                       depends_on = NULL, export = NULL) {
      self$enqueue_(substitute(expr), envir, queue,
                    separate_process, timeout_task_run, depends_on, export)
    },

    ##' @description Queue an expression
    ##'
    ##' @param expr Any R expression, quoted; use this to use `$enqueue`
    ##' in a programmatic context where you want to construct expressions
    ##' directly (e.g., `bquote(log(.(x)), list(x = 10))`
    ##'
    ##' @param envir The environment that you would run this expression in
    ##'   locally. This will be used to copy across any dependent variables.
    ##'   For example, if your expression is `sum(1 + a)`, we will also send
    ##'   the value of `a` to the worker along with the expression.
    ##
    ##' @param queue The queue to add the task to; if not specified the
    ##'   "default" queue (which all workers listen to) will be
    ##'   used. If you have configured workers to listen to more than
    ##'   one queue you can specify that here. Be warned that if you
    ##'   push jobs onto a queue with no worker, it will queue forever.
    ##'
    ##' @param separate_process Logical, indicating if the task should be
    ##'   run in a separate process on the worker (see `$enqueue` for
    ##'   details).
    ##'
    ##' @param timeout_task_run Optionally, a maximum allowed running time, in
    ##'   seconds (see `$enqueue` for details).
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue.
    ##'
    ##' @param export Optionally a list of variables to export for the
    ##'   calculation. See `$enqueue` for details.
    enqueue_ = function(expr, envir = parent.frame(),
                        queue = NULL, separate_process = FALSE,
                        timeout_task_run = NULL,
                        depends_on = NULL, export = NULL) {
      task_id <- ids::random_id()
      verify_dependencies_exist(self, depends_on)
      dat <- expression_prepare(expr, envir, private$store, task_id,
                                export = export)
      task_submit(self$to_v2(), task_id, dat, queue,
                  separate_process, timeout_task_run, depends_on)
    },

    ##' @description Apply a function over a list of data. This is
    ##' equivalent to using `$enqueue()` over each element in the list.
    ##'
    ##' @param X A list of data to apply our function against
    ##'
    ##' @param FUN A function to be applied to each element of `X`
    ##'
    ##' @param ... Additional arguments to `FUN`
    ##'
    ##' @param dots As an alternative to `...`, you can provide the dots
    ##'   as a list of additional arguments. This may be easier to program
    ##'   against.
    ##'
    ##' @param envir The environment to use to try and find the function
    ##'
    ##' @param timeout_task_wait Optional timeout, in seconds, after which an
    ##'   error will be thrown if all tasks have not completed. If given  as
    ##'   `0`, then we return a handle that can be used to check for tasks
    ##'   using `bulk_wait`. If not given, falls back on the controller's
    ##'   `timeout_task_wait` (see `$new()`)
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion (default is 1s, see `$task_wait()` for details)
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param queue The queue to add the tasks to (see `$enqueue` for
    ##'   details).
    ##'
    ##' @param separate_process Logical, indicating if the task should be
    ##'   run in a separate process on the worker (see `$enqueue` for
    ##'   details).
    ##'
    ##' @param timeout_task_run Optionally, a maximum allowed running time, in
    ##'   seconds (see the `timeout` argument of `$enqueue` for details).
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue. Dependencies are applied to all
    ##'   tasks added to the queue.
    ##'
    ##' @param delete Optional logical, indicating if the tasks
    ##'   should be be immediately deleted after collection, preventing
    ##'   buildup of lots of content in your Redis database.
    ##'
    ##' @param error Optional logical, indicating if an error in the task
    ##'   should throw. Like `$task_result()` the default is not to throw,
    ##'   giving you back an `rrq_task_error` object for each failing task.
    ##'   If `error = TRUE` we throw on error instead.
    lapply = function(X, FUN, ..., dots = NULL, # nolint
                      envir = parent.frame(), queue = NULL,
                      separate_process = FALSE, timeout_task_run = NULL,
                      depends_on = NULL,
                      timeout_task_wait = NULL, time_poll = 1,
                      progress = NULL, delete = FALSE, error = FALSE) {
      if (is.null(dots)) {
        dots <- as.list(substitute(list(...)))[-1L]
      }
      self$lapply_(X, substitute(FUN), dots = dots, envir = envir,
                   queue = queue, separate_process = separate_process,
                   timeout_task_run = timeout_task_run,
                   depends_on = depends_on,
                   timeout_task_wait = timeout_task_wait,
                   time_poll = time_poll, progress = progress, delete = delete,
                   error = error)
    },

    ##' @description The "standard evaluation" version of `$lapply()`.
    ##' This differs in how the function is found and how dots are passed.
    ##' With this version, both are passed by value; this may create more
    ##' overhead on the redis server as the values of the variables will
    ##' be copied over rather than using their names if possible.
    ##'
    ##' @param X A list of data to apply our function against
    ##'
    ##' @param FUN A function to be applied to each element of `X`
    ##'
    ##' @param ... Additional arguments to `FUN`
    ##'
    ##' @param dots As an alternative to `...`, you can provide the dots
    ##'   as a list of additional arguments. This may be easier to program
    ##'   against.
    ##'
    ##' @param envir The environment to use to try and find the function
    ##'
    ##' @param timeout_task_wait Optional timeout, in seconds, after which an
    ##'   error will be thrown if all tasks have not completed. If given  as
    ##'   `0`, then we return a handle that can be used to check for tasks
    ##'   using `bulk_wait`. If not given, falls back on the controller's
    ##'   `timeout_task_wait` (see `$new()`)
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion (default is 1s, see `$task_wait()` for details)
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param queue The queue to add the tasks to (see `$enqueue` for
    ##'   details).
    ##'
    ##' @param separate_process Logical, indicating if the task should be
    ##'   run in a separate process on the worker (see `$enqueue` for
    ##'   details).
    ##'
    ##' @param timeout_task_run Optionally, a maximum allowed running time, in
    ##'   seconds (see the `timeout` argument of `$enqueue` for details).
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue. Dependencies are applied to all
    ##'   tasks added to the queue.
    ##'
    ##' @param delete Optional logical, indicating if the tasks
    ##'   should be be immediately deleted after collection, preventing
    ##'   buildup of lots of content in your Redis database.
    ##'
    ##' @param error Optional logical, indicating if an error in the task
    ##'   should throw. Like `$task_result()` the default is not to throw,
    ##'   giving you back an `rrq_task_error` object for each failing task.
    ##'   If `error = TRUE` we throw on error instead.
    lapply_ = function(X, FUN, ..., dots = NULL, # nolint
                       envir = parent.frame(), queue = NULL,
                       separate_process = FALSE, timeout_task_run = NULL,
                       depends_on = NULL, timeout_task_wait = NULL,
                       time_poll = 1, progress = NULL, delete = FALSE,
                       error = FALSE) {
      if (is.null(dots)) {
        dots <- list(...)
      }
      timeout_task_wait <- timeout_task_wait %||% private$timeout_task_wait
      rrq_lapply(self$to_v2(), X, FUN, dots, envir,
                 queue, separate_process, timeout_task_run, depends_on,
                 timeout_task_wait, time_poll, progress, delete, error)
    },

    ##' @description Send a bulk set of tasks to your workers.
    ##' This function is a bit like a mash-up of [Map] and [do.call],
    ##' when used with a [data.frame] argument, which is typically what
    ##' is provided. Rather than `$lapply()` which applies `FUN` to each
    ##' element of `X`, `enqueue_bulk will apply over each row of `X`,
    ##' spreading the columms out as arguments. If you have a function
    ##' `f(a, b)` and a [data.frame] with columns `a` and `b` this
    ##' should feel intuitive.
    ##'
    ##' @param X Typically a [data.frame], which you want to apply `FUN`
    ##'   over, row-wise. The names of the `data.frame` must match the
    ##'   arguments of your function.
    ##'
    ##' @param FUN A function
    ##'
    ##' @param ... Additional arguments to add to every call to `FUN`
    ##'
    ##' @param dots As an alternative to `...`, you can provide the dots
    ##'   as a list of additional arguments. This may be easier to program
    ##'   against.
    ##'
    ##' @param envir The environment to use to try and find the function
    ##'
    ##' @param timeout_task_wait Optional timeout, in seconds, after which an
    ##'   error will be thrown if all tasks have not completed. If given  as
    ##'   `0`, then we return a handle that can be used to check for tasks
    ##'   using `bulk_wait`. If not given, falls back on the controller's
    ##'   `timeout_task_wait` (see `$new()`)
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion (default is 1s, see `$task_wait()` for details)
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param queue The queue to add the tasks to (see `$enqueue` for
    ##'   details).
    ##'
    ##' @param separate_process Logical, indicating if the task should be
    ##'   run in a separate process on the worker (see `$enqueue` for
    ##'   details).
    ##'
    ##' @param timeout_task_run Optionally, a maximum allowed running time, in
    ##'   seconds (see the `timeout` argument of `$enqueue` for details).
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue. Dependencies are applied to all
    ##'   tasks added to the queue.
    ##'
    ##' @param delete Optional logical, indicating if the tasks
    ##'   should be be immediately deleted after collection, preventing
    ##'   buildup of lots of content in your Redis database.
    ##'
    ##' @param error Optional logical, indicating if an error in the task
    ##'   should throw. Like `$task_result()` the default is not to throw,
    ##'   giving you back an `rrq_task_error` object for each failing task.
    ##'   If `error = TRUE` we throw on error instead.
    enqueue_bulk = function(X, FUN, ..., dots = NULL, # nolint
                            envir = parent.frame(), queue = NULL,
                            separate_process = FALSE, timeout_task_run = NULL,
                            depends_on = NULL, timeout_task_wait = NULL,
                            time_poll = 1, progress = NULL, delete = FALSE,
                            error = FALSE) {
      if (is.null(dots)) {
        dots <- as.list(substitute(list(...)))[-1L]
      }
      self$enqueue_bulk_(X, substitute(FUN), dots = dots, envir = envir,
                         queue = queue, separate_process = separate_process,
                         timeout_task_run = timeout_task_run,
                         depends_on = depends_on,
                         timeout_task_wait = timeout_task_wait,
                         time_poll = time_poll, progress = progress,
                         delete = delete, error = error)
    },

    ##' @description The "standard evaluation" version of `$enqueue_bulk()`.
    ##' This differs in how the function is found and how dots are passed.
    ##' With this version, both are passed by value; this may create more
    ##' overhead on the redis server as the values of the variables will
    ##' be copied over rather than using their names if possible.
    ##'
    ##' @param X Typically a [data.frame], which you want to apply `FUN`
    ##'   over, row-wise. The names of the `data.frame` must match the
    ##'   arguments of your function.
    ##'
    ##' @param FUN A function
    ##'
    ##' @param ... Additional arguments to add to every call to `FUN`
    ##'
    ##' @param dots As an alternative to `...`, you can provide the dots
    ##'   as a list of additional arguments. This may be easier to program
    ##'   against.
    ##'
    ##' @param envir The environment to use to try and find the function
    ##'
    ##' @param timeout_task_wait Optional timeout, in seconds, after which an
    ##'   error will be thrown if all tasks have not completed. If given  as
    ##'   `0`, then we return a handle that can be used to check for tasks
    ##'   using `bulk_wait`. If not given, falls back on the controller's
    ##'   `timeout_task_wait` (see `$new()`)
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion (default is 1s, see `$task_wait()` for details)
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param queue The queue to add the tasks to (see `$enqueue` for
    ##'   details).
    ##'
    ##' @param separate_process Logical, indicating if the task should be
    ##'   run in a separate process on the worker (see `$enqueue` for
    ##'   details).
    ##'
    ##' @param timeout_task_run Optionally, a maximum allowed running time, in
    ##'   seconds (see the `timeout` argument of `$enqueue` for details).
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue. Dependencies are applied to all
    ##'   tasks added to the queue.
    ##'
    ##' @param delete Optional logical, indicating if the tasks
    ##'   should be be immediately deleted after collection, preventing
    ##'   buildup of lots of content in your Redis database.
    ##'
    ##' @param error Optional logical, indicating if an error in the task
    ##'   should throw. Like `$task_result()` the default is not to throw,
    ##'   giving you back an `rrq_task_error` object for each failing task.
    ##'   If `error = TRUE` we throw on error instead.
    enqueue_bulk_ = function(X, FUN, ..., dots = NULL, # nolint
                             envir = parent.frame(), queue = NULL,
                             separate_process = FALSE, timeout_task_run = NULL,
                             depends_on = NULL, timeout_task_wait = NULL,
                             time_poll = 1, progress = NULL, delete = delete,
                             error = error) {
      if (is.null(dots)) {
        dots <- list(...)
      }
      timeout_task_wait <- timeout_task_wait %||% private$timeout_task_wait
      rrq_enqueue_bulk(self$to_v2(), X, FUN, dots,
                       envir, queue, separate_process, timeout_task_run,
                       depends_on, timeout_task_wait, time_poll,
                       progress, delete, error)
    },

    ##' @description Wait for a group of tasks
    ##'
    ##' @param x An object of class `rrq_bulk`, as created by `$lapply()`
    ##'
    ##' @param timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if the task has not completed.
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion (default is 1s, see `$task_wait()` for details)
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param delete Optional logical, indicating if the tasks
    ##'   should be be immediately deleted after collection, preventing
    ##'   buildup of lots of content in your Redis database.
    ##'
    ##' @param error Optional logical, indicating if an error in the task
    ##'   should throw. Like `$task_result()` the default is not to throw,
    ##'   giving you back an `rrq_task_error` object for each failing task.
    ##'   If `error = TRUE` we throw on error instead.
    bulk_wait = function(x, timeout = NULL, time_poll = 1,
                         progress = NULL, delete = FALSE, error = FALSE,
                         follow = NULL) {
      timeout <- timeout %||% private$timeout_task_wait
      rrq_bulk_wait(self$to_v2(), x, timeout,
                    time_poll, progress, delete, error,
                    follow %||% private$follow)
    },

    ##' @description Return a character vector of task statuses. The name
    ##' of each element corresponds to a task id, and the value will be
    ##' one of the possible statuses ("PENDING", "COMPLETE", etc).
    ##'
    ##' @param task_ids Optional character vector of task ids for which you
    ##' would like statuses. If not given (or `NULL`) then the status of
    ##' all task ids known to this rrq controller is returned.
    task_status = function(task_ids = NULL, follow = NULL) {
      named <- TRUE
      rrq_task_status(task_ids %||% self$task_list(), named, follow, self)
    },

    ##' @description Get the result for a single task (see `$tasks_result`
    ##'   for a method for efficiently getting multiple results at once).
    ##'   Returns the value of running the task if it is complete, and an
    ##'   error otherwise.
    ##'
    ##' @param task_id The single id for which the result is wanted.
    ##'
    ##' @param error Logical, indicating if we should throw an error
    ##'   if a task was not successful. By default (`error = FALSE`),
    ##'   in the case of the task result returning an error we return
    ##'   an object of class `rrq_task_error`, which contains information
    ##'   about the error. Passing `error = TRUE` simply calls `stop()`
    ##'   on this error if it is returned.
    task_result = function(task_id, error = FALSE, follow = NULL) {
      rrq_task_result(task_id, error, follow, self)
    },

    ##' @description Get the results of a group of tasks, returning them as a
    ##' list.
    ##'
    ##' @param task_ids A vector of task ids for which the task result
    ##' is wanted.
    ##'
    ##' @param error Logical, indicating if we should throw an error if
    ##'   the task was not successful. See `$task_result()` for details.
    tasks_result = function(task_ids, error = FALSE, follow = NULL) {
      named <- TRUE
      rrq_task_results(task_ids, error, named, follow, self)
    },

    ##' @description Poll for a task to complete, returning the result
    ##' when completed. If the task has already completed this is
    ##' roughly equivalent to `task_result`. See `$tasks_wait` for an
    ##' efficient way of doing this for a group of tasks.
    ##'
    ##' @param task_id The single id that we will wait for
    ##'
    ##' @param timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if the task has not completed. If not given,
    ##'   falls back on the controller's `timeout_task_wait` (see `$new()`)
    ##'
    ##' @param time_poll Optional time with which to "poll" for completion.
    ##'   By default this will be 1 second; this is the time that each
    ##'   request for a completed task may block for (however, if the task
    ##'   is finished before this, the actual time waited for will be less).
    ##'   Increasing this will reduce the responsiveness of your R session
    ##'   to interrupting, but will cause slightly less network load.
    ##'   Values less than 1s are not currently supported as this requires
    ##'   a very recent Redis server.
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param error Logical, indicating if we should throw an error if
    ##'   the task was not successful. See `$task_result()` for details.
    ##'   Note that an error is always thrown if not all tasks are fetched
    ##'   in time.
    task_wait = function(task_id, timeout = NULL, time_poll = 1,
                         progress = NULL, error = FALSE, follow = NULL) {
      rrq_task_wait(task_id, timeout, time_poll, progress, follow, self)
      self$task_result(task_id, error = error, follow = follow)
    },

    ##' @description Retry a task (or set of tasks). Typically this
    ##' is after failure (e.g., `ERROR`, `DIED` or similar) but you can
    ##' retry even successfully completed tasks. Once retried, methods
    ##' that retrieve information about a task (e.g., `$task_status()`,
    ##' `$task_result()`) will behave differently depending on the value
    ##' of their `follow` argument. See `vignette("fault-tolerance")`
    ##' for more details.
    ##'
    ##' @param task_ids Task ids to retry.
    task_retry = function(task_ids) {
      rrq_task_retry(task_ids, self)
    },

    ##' @description Returns the number of tasks in the queue (waiting for
    ##' an available worker).
    ##'
    ##' @param queue The name of the queue to query (defaults to the
    ##'   "default" queue).
    queue_length = function(queue = NULL) {
      rrq_queue_length(queue, self)
    },

    ##' @description Returns the keys in the task queue.
    ##'
    ##' @param queue The name of the queue to query (defaults to the
    ##'   "default" queue).
    queue_list = function(queue = NULL) {
      rrq_queue_list(queue, self)
    },

    ##' @description Returns the last (few) elements in the worker
    ##' log. The log will be returned as a [data.frame] of entries
    ##' `worker_id` (the worker id), `child` (the process id, an integer,
    ##' where logs come from a child process from a task queued with
    ##' `separate_process = TRUE`), `time` (the time in Redis when the
    ##' event happened; see [redux::redis_time] to convert this to an R
    ##' time), `command` (the worker command) and `message` (the message
    ##' corresponding to that command).
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' all active workers are used.
    ##'
    ##' @param n Number of elements to select, the default being the single
    ##' last entry. Use `Inf` or `0` to indicate that you want all log entries
    worker_log_tail = function(worker_ids = NULL, n = 1) {
      rrq_worker_log_tail(worker_ids, n, self)
    },

    ##' @description Stop workers.
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' all active workers will be stopped.
    ##'
    ##' @param type The strategy used to stop the workers. Can be `message`,
    ##'   `kill` or `kill_local` (see details).
    ##'
    ##' @param timeout Optional timeout; if greater than zero then we poll
    ##'   for a response from the worker for this many seconds until they
    ##'   acknowledge the message and stop (only has an effect if `type`
    ##'   is `message`). If a timeout of greater than zero is given, then
    ##'   for a `message`-based stop we wait up to this many seconds for the
    ##'   worker to exit. That means that we might wait up to `2 * timeout`
    ##'   seconds for this function to return.
    ##'
    ##' @param time_poll If `type` is `message` and `timeout` is greater
    ##'   than zero, this is the polling interval used between redis calls.
    ##'   Increasing this reduces network load but decreases the ability
    ##'   to interrupt the process.
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @details The `type` parameter indicates the strategy used to stop
    ##' workers, and interacts with other parameters. The strategies used by
    ##' the different values are:
    ##'
    ##' * `message`, in which case a `STOP` message will be sent to the
    ##'   worker, which they will receive after finishing any currently
    ##'   running task (if `RUNNING`; `IDLE` workers will stop immediately).
    ##' * `kill`, in which case a kill signal will be sent via the heartbeat
    ##'   (if the worker is using one). This will kill the worker even if
    ##'   is currently working on a task, eventually leaving that task with
    ##'   a status of `DIED`.
    ##' * `kill_local`, in which case a kill signal is sent using operating
    ##'    system signals, which requires that the worker is on the same
    ##'    machine as the controller.
    worker_stop = function(worker_ids = NULL, type = "message",
                           timeout = 0, time_poll = 0.05, progress = NULL) {
      rrq_worker_stop(worker_ids, type, timeout, time_poll, progress, self)
    },

    ##' @description Detects exited workers through a lapsed heartbeat
    worker_detect_exited = function() {
      rrq_worker_detect_exited(self)
    },

    ##' @description Save a worker configuration, which can be used to
    ##' start workers with a set of options with the cli. These
    ##' correspond to arguments to [rrq::rrq_worker].
    ##'
    ##' @param name Name for this configuration
    ##'
    ##' @param config A worker configuration, created by
    ##'   [rrq::rrq_worker_config()]
    ##'
    ##' @param overwrite Logical, indicating if an existing configuration
    ##'   with this `name` should be overwritten if it exists. If `FALSE`,
    ##'   then the configuration is not updated, even if it differs from
    ##'   the version currently saved.
    ##'
    ##' @return Invisibly, a boolean indicating if the configuration was
    ##'   updated.
    worker_config_save = function(name, config, overwrite = TRUE) {
      rrq_worker_config_save2(name, config, overwrite, self)
    }
  ),

  private = list(
    keys = NULL,
    timeout_task_wait = NULL,
    follow = NULL,
    scripts = NULL,
    store = NULL
  ))



task_submit <- function(controller, task_id, dat, queue,
                        separate_process, timeout, depends_on = NULL) {
  task_submit_n(controller, task_id, list(object_to_bin(dat)), NULL,
                queue, separate_process, timeout, depends_on)
}


task_submit_n <- function(controller, task_ids, dat, key_complete, queue,
                          separate_process, timeout, depends_on = NULL) {
  con <- controller$con
  keys <- controller$keys

  n <- length(dat)
  queue <- queue %||% QUEUE_DEFAULT
  key_queue <- rrq_key_queue(keys$queue_id, queue)

  local <- if (separate_process) "FALSE" else "TRUE"

  if (!is.null(timeout)) {
    if (!separate_process) {
      stop("Can't set timeout as 'separate_process' is FALSE")
    }
    timeout <- list(
      redis$HMSET(keys$task_timeout, task_ids, as.character(timeout)))
  }

  depends_up_original_keys <- rrq_key_task_depends_up_original(
    keys$queue_id, task_ids)
  depends_up_keys <- rrq_key_task_depends_up(keys$queue_id, task_ids)
  depends_down_keys <- rrq_key_task_depends_down(keys$queue_id, depends_on)

  if (!is.null(key_complete)) {
    cmds <- list(
      redis$HMSET(keys$task_complete, task_ids, rep_len(key_complete, n)))
  } else {
    cmds <- list()
  }

  time <- timestamp()
  cmds <- c(
    cmds,
    list(
      redis$HMSET(keys$task_expr, task_ids, dat),
      redis$HMSET(keys$task_status, task_ids, rep_len(TASK_PENDING, n)),
      redis$HMSET(keys$task_queue, task_ids, rep_len(queue, n)),
      redis$HMSET(keys$task_local, task_ids, rep_len(local, n)),
      redis$HMSET(keys$task_time_submit, task_ids, rep_len(time, n))),
    timeout)
  if (length(depends_on) > 0) {
    cmds <- c(
      cmds,
      list(
        status = redis$HMGET(keys$task_status, depends_on),
        redis$HMSET(keys$task_status, task_ids, rep_len(TASK_DEFERRED, n))),
      lapply(depends_up_original_keys, redis$SADD, depends_on),
      lapply(depends_up_keys, redis$SADD, depends_on),
      lapply(depends_down_keys, redis$SADD, task_ids))
  } else {
    cmds <- c(cmds, list(redis$RPUSH(key_queue, task_ids)))
  }
  response <- con$pipeline(.commands = cmds)

  ## If any dependencies will never be satisfied then cleanup and error
  ## We do it this way around i.e. queue then check status of dependencies to
  ## avoid a race condition. If we were to check status of dependencies
  ## then queue we could get into condition where e.g.
  ## 1. Run report B which depends on report A
  ## 2. Check status of A and it is running
  ## 3. Add B to the queue
  ## In the time between 2 and 3 A could have finished and failed meaning that
  ## the dependency of B will never be satisfied and it will never be run.
  if (any(response$status %in% TASK$terminal_fail)) {
    run_task_cleanup_failure(controller, task_ids, TASK_IMPOSSIBLE, NULL)
    incomplete <- response$status[response$status %in% TASK$terminal_fail]
    names(incomplete) <- depends_on[response$status %in% TASK$terminal_fail]
    stop(sprintf("Failed to queue as dependent tasks failed:\n%s",
                 paste0(paste0(names(incomplete), ": ", incomplete),
                        collapse = ", ")))
  }

  complete <- depends_on[response$status == TASK_COMPLETE]
  for (dep_id in complete) {
    queue_dependencies(controller, dep_id, task_ids)
  }

  task_ids
}


controller_info <- function() {
  list(hostname = hostname(),
       pid = process_id(),
       username = username(),
       time = Sys.time())
}

worker_naturalsort <- function(x) {
  re <- "^(.*)_(\\d+)$"
  root <- sub(re, "\\1", x)
  i <- grepl(re, x)
  idx <- numeric(length(x))
  idx[i] <- as.integer(sub(re, "\\2", x[i]))
  x[order(root, idx)]
}


tasks_wait <- function(controller, task_ids, timeout, time_poll,
                       progress, key_complete, error, follow,
                       call = NULL) {
  con <- controller$con
  keys <- controller$keys

  ## This can be relaxed in recent Redis >= 6.0.0 as we then interpret
  ## time_poll as a double. To do this efficiently we'll want to get
  ## the version information stored into the redux client, which is
  ## not hard as we already do some negotiation
  time_poll <- validate_time_poll(con, time_poll, call)
  if (follow) {
    task_ids_from <- task_follow(controller, task_ids)
  } else {
    task_ids_from <- task_ids
  }

  done <- set_names(
    hash_exists(con, keys$task_result, task_ids_from, TRUE),
    task_ids_from)

  fetch <- function() {
    tmp <- con$BLPOP(key_complete, time_poll)
    if (!is.null(tmp)) {
      done[[tmp[[2L]]]] <<- TRUE
    }
    done
  }

  if (!all(done)) {
    general_poll(fetch, 0, timeout, "tasks", TRUE, progress)
  }

  ## A bit inefficient, but we won't do it this way for long:
  controller <- rrq_controller2(keys$queue_id, con)
  rrq_task_results(task_ids_from, error = error, controller = controller)
}


rrq_object_store <- function(con, keys) {
  config <- rrq_configure_read(con, keys)
  if (is.null(config$offload_path)) {
    offload <- object_store_offload_null$new()
  } else {
    offload <- object_store_offload_disk$new(config$offload_path)
  }
  object_store$new(con, keys$object_store,
                   config$store_max_size, offload)
}


verify_dependencies_exist <- function(controller, depends_on) {
  if (!is.null(depends_on)) {
    dependencies_exist <- rrq_task_exists(depends_on, named = TRUE,
                                          controller = controller)
    if (!all(dependencies_exist)) {
      missing <- names(dependencies_exist[!dependencies_exist])
      error_msg <- ngettext(
        length(missing),
        "Failed to queue as dependency %s does not exist.",
        "Failed to queue as dependencies %s do not exist.")
      stop(sprintf(error_msg, paste0(missing, collapse = ", ")))
    }
  }
  invisible(TRUE)
}


throw_task_errors <- function(res) {
  is_error <- vlapply(res, inherits, "rrq_task_error")
  if (any(is_error)) {
    stop(rrq_task_error_group(unname(res[is_error]), length(res)))
  }
}
