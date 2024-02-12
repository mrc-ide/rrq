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
##' @section Bulk interface (`lapply`):
##'
##' The bulk interface is a bit more complicated than the basic
##'   `enqueue` interface. In the majority of cases you can ignore the
##'   details and use the `lapply` method in much the same way as you
##'   would in normal R. Assuming that `obj` is your `rrq_controller`
##'   object, you might write:
##'
##' ```
##' ans <- obj$lapply(1:10, sqrt)
##' ```
##'
##' which will return the same thing as `lapply(1:10, sqrt)` (provided
##'   that you have a Redis server running and workers registered)
##'
##' There is some sleight of hand here, though as we need to identify
##'   that it is the *symbol* `sqrt` that matters there corresponding
##'   to the builtin [sqrt] function. You can make this more explicit
##'   by passing in the name of the function using `$lapply_()`
##'
##' ```
##' ans <- obj$lapply(1:10, quote(sqrt))
##' ```
##'
##' The same treatment applies to the dots; this is allowed:
##'
##' ```
##' b <- 2
##' ans <- obj$lapply(1:10, log, base = b)
##' ```
##'
##' But this will look up the bindings of `log` and `b` in the context
##'   in which the call is made. This may not always do what is
##'   expected, so you can use the names directly:
##'
##' ```
##' b <- 2
##' ans <- obj$lapply_(1:10, quote(log), base = quote(b))
##' ```
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
      ret <- list(queue_id = self$queue_id,
                  con = self$con,
                  keys = private$keys,
                  timeout_task_wait = private$timeout_task_wait,
                  follow = private$follow,
                  scripts = private$scripts,
                  store = private$store)
      class(ret) <- "rrq_controller2"
      ret
    },

    ##' @description Entirely destroy a queue, by deleting all keys
    ##' associated with it from the Redis database. This is a very
    ##' destructive action and cannot be undone.
    ##'
    ##' @param delete Either `TRUE` (the default) indicating that the
    ##'   keys should be immediately deleted. Alternatively, provide an
    ##'   integer value and the keys will instead be marked for future
    ##'   deletion by "expiring" after this many seconds, using Redis'
    ##'   `EXPIRE` command.
    ##'
    ##' @param worker_stop_type Passed to `$worker_stop`; Can be one of
    ##'   "message", "kill" or "kill_local". The "kill" method requires that
    ##'   the workers are using a heartbeat, and "kill_local" requires that
    ##'   the workers are on the same machine as the controller. However,
    ##'   these may be faster to stop workers than "message", which will
    ##'   wait until any task is finished.
    ##'
    ##' @param timeout_worker_stop A timeout to pass to the worker to
    ##'   respond the request to stop. See `worker_stop`'s `timeout`
    ##'   argument for details.
    destroy = function(delete = TRUE, worker_stop_type = "message",
                       timeout_worker_stop = 0) {
      if (!is.null(self$con)) {
        rrq_destroy(delete, worker_stop_type, timeout_worker_stop, self)
        ## render the controller useless:
        self$con <- NULL
        private$keys <- NULL
      }
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
      task_submit(self$con, private$keys, private$store, task_id, dat, queue,
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
      rrq_lapply(self$con, private$keys, private$store, X, FUN, dots, envir,
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
      rrq_enqueue_bulk(self$con, private$keys, private$store, X, FUN, dots,
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
      rrq_bulk_wait(self$con, private$keys, private$store, x, timeout,
                    time_poll, progress, delete, error,
                    follow %||% private$follow)
    },

    ##' @description List ids of all tasks known to this rrq controller
    task_list = function() {
      rrq_task_list(self)
    },

    ##' @description Test if task with id `task_ids` is known to this
    ##'   rrq controller
    ##' @param task_ids Character vector of task ids to check for existence.
    task_exists = function(task_ids) {
      rrq_task_exists(task_ids, self)
    },

    ##' @description Return a character vector of task statuses. The name
    ##' of each element corresponds to a task id, and the value will be
    ##' one of the possible statuses ("PENDING", "COMPLETE", etc).
    ##'
    ##' @param task_ids Optional character vector of task ids for which you
    ##' would like statuses. If not given (or `NULL`) then the status of
    ##' all task ids known to this rrq controller is returned.
    task_status = function(task_ids = NULL, follow = NULL) {
      rrq_task_status(task_ids, follow, self)
    },

    ##' @description Retrieve task progress, if set. This will be `NULL`
    ##'   if progress has never been registered, otherwise whatever value
    ##'   was set - can be an arbitrary R object.
    ##'
    ##' @param task_id A single task id for which the progress is wanted.
    task_progress = function(task_id) {
      rrq_task_progress(task_id, self)
    },

    ##' @description Provide a high level overview of task statuses
    ##' for a set of task ids, being the count in major categories of
    ##' `PENDING`, `RUNNING`, `COMPLETE` and `ERROR`.
    ##'
    ##' @param task_ids Optional character vector of task ids for which you
    ##' would like the overview. If not given (or `NULL`) then the status of
    ##' all task ids known to this rrq controller is used.
    task_overview = function(task_ids = NULL) {
      rrq_task_overview(task_ids, self)
    },

    ##' @description Find the position of one or more tasks in the queue.
    ##'
    ##' @param task_ids Character vector of tasks to find the position for.
    ##'
    ##' @param missing Value to return if the task is not found in the queue.
    ##'   A task will take value `missing` if it is running, complete,
    ##'   errored, deferred etc and a positive integer if it is in the queue,
    ##'   indicating its position (with 1) being the next task to run.
    ##'
    ##' @param queue The name of the queue to query (defaults to the
    ##'   "default" queue).
    task_position = function(task_ids, missing = 0L, queue = NULL,
                             follow = NULL) {
      rrq_task_position(task_ids, missing, queue, follow, self)
    },

    ##' @description List the tasks in front of `task_id` in the queue.
    ##'   If the task is missing from the queue this will return NULL. If
    ##'   the task is next in the queue this will return an empty character
    ##'   vector.
    ##'
    ##' @param task_id Task to find the position for.
    ##'
    ##' @param queue The name of the queue to query (defaults to the
    ##'   "default" queue).
    task_preceeding = function(task_id, queue = NULL, follow = NULL) {
      rrq_task_preceeding(task_id, queue, follow, self)
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
      rrq_task_results(task_ids, error, follow, self)
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
      self$task_result(task_ids, error = error, follow = follow)
    },

    ##' @description Poll for a group of tasks to complete, returning the
    ##' result as list when completed. If the tasks have already completed
    ##' this is roughly equivalent to `tasks_result`.
    ##'
    ##' @param task_ids A vector of task ids to poll for
    ##'
    ##' @param timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if the task has not completed. If not given,
    ##'   falls back on the controller's `timeout_task_wait` (see `$new()`)
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion (default is 1s, see `$task_wait()` for details)
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
    tasks_wait = function(task_ids, timeout = NULL, time_poll = 1,
                          progress = NULL, error = FALSE, follow = NULL) {
      rrq_task_wait(task_ids, timeout, time_poll, progress, follow, self)
      self$tasks_result(task_ids, error = error, follow = follow)
    },

    ##' @description Delete one or more tasks
    ##'
    ##' @param task_ids Vector of task ids to delete
    ##'
    ##' @param check Logical indicating if we should check that the tasks
    ##'   are not running. Deleting running tasks is unlikely to result in
    ##'   desirable behaviour.
    task_delete = function(task_ids, check = TRUE) {
      rrq_task_delete(task_ids, check, self)
    },

    ##' @description Cancel a single task. If the task is `PENDING` it
    ##' will be unqueued and the status set to `CANCELED`.  If `RUNNING`
    ##' then the task will be stopped if it was set to run in
    ##' a separate process (i.e., queued with `separate_process = TRUE`).
    ##' Dependent tasks will be marked as impossible.
    ##'
    ##' @param task_id Id of the task to cancel
    ##'
    ##' @param wait Wait for the task to be stopped, if it was running.
    ##'
    ##' @param timeout_wait Maximum time, in seconds, to wait for the task
    ##'   to be cancelled by the worker.
    ##'
    ##' @return Nothing if successfully cancelled, otherwise throws an
    ##' error with task_id and status e.g. Task 123 is not running (MISSING)
    task_cancel = function(task_id, wait = TRUE, timeout_wait = 10) {
      rrq_task_cancel(task_id, wait, timeout_wait, self)
    },

    ##' @description Fetch internal data about a task from Redis
    ##' (expert use only).
    ##'
    ##' @param task_id The id of the task
    task_data = function(task_id) {
      rrq_task_data(task_id, self)
    },

    ##' @description Return information about a task. This currently
    ##' includes information about where a task is (or was) running
    ##' and information about any retry chain,
    ##' but will expand in future. The format of the output here is
    ##' subject to change (and will probably get a nice print method)
    ##' but the values present in the output will be included in any
    ##' future update.
    ##'
    ##' @param task_id The id of the task to fetch information about
    task_info = function(task_id) {
      rrq_task_info(task_id, self)
    },

    ##' @description Fetch times for tasks at points in their life cycle.
    ##' For each task returns the time of submission, starting
    ##' and completion (not necessarily successfully; this includes
    ##' errors and interruptions).  If a task has not reached a point
    ##' yet (e.g., submitted but not run, or running but not finished)
    ##' the time will be `NA`).  Times are returned in unix timestamp
    ##' format in UTC; you can use [redux::redis_time_to_r] to convert
    ##' them to a POSIXt object.
    ##'
    ##' @param task_ids Task ids to fetch times for.
    task_times = function(task_ids, follow = NULL) {
      rrq_task_times(task_ids, follow, self)
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
      self$con$LLEN(rrq_key_queue(private$keys$queue_id, queue))
    },

    ##' @description Returns the keys in the task queue.
    ##'
    ##' @param queue The name of the queue to query (defaults to the
    ##'   "default" queue).
    queue_list = function(queue = NULL) {
      rrq_queue_list(queue, self)
    },

    ##' @description Remove task ids from a queue.
    ##'
    ##' @param task_ids Task ids to remove
    ##'
    ##' @param queue The name of the queue to query (defaults to the
    ##'   "default" queue).
    queue_remove = function(task_ids, queue = NULL) {
      ## NOTE: uses a pipeline to avoid a race condition - nothing may
      ## interere with the queue between the LRANGE and the DEL or we
      ## might lose tasks or double-queue them. If a job is queued
      ## between the DEL and the RPUSH the newly submitted job gets
      ## bounced ahead in the queue, which seems tolerable but might not
      ## always be ideal.  To solve this we should use a lua script.
      rrq_queue_remove(task_ids, queue, self)
    },

    ##' @description Return deferred tasks and what they are waiting on.
    ##'   Note this is in an arbitrary order, tasks will be added to the
    ##'   queue as their dependencies are satisfied.
    deferred_list = function() {
      rrq_deferred_list(self)
    },

    ##' @description Returns the number of active workers
    worker_len = function() {
      rrq_worker_len(self)
    },

    ##' @description Returns the ids of active workers
    worker_list = function() {
      rrq_worker_list(self)
    },

    ##' @description Returns the ids of workers known to have exited
    worker_list_exited = function() {
      rrq_worker_list_exited(self)
    },

    ##' @description Returns a list of information about active
    ##' workers (or exited workers if `worker_ids` includes them).
    ##
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' all active workers are used.
    worker_info = function(worker_ids = NULL) {
      rrq_worker_info(worker_ids, self)
    },

    ##' @description Returns a character vector of current worker statuses
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' all active workers are used.
    worker_status = function(worker_ids = NULL) {
      rrq_worker_status(worker_ids, self)
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

    ##' @description Cleans up workers known to have exited
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' rrq looks for exited workers.
    worker_delete_exited = function(worker_ids = NULL) {
      rrq_worker_delete_exited(worker_ids, self)
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

    ##' @description Return the contents of a worker's process log, if
    ##' it is located on the same physical storage (including network
    ##' storage) as the controller. This will generally behave for
    ##' workers started with [rrq_worker_spawn] but may require significant
    ##' care otherwise.
    ##'
    ##' @param worker_id The worker for which the log is required
    worker_process_log = function(worker_id) {
      rrq_worker_process_log(worker_id, self)
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
    },

    ##' @description Return names of worker configurations saved by
    ##' `$worker_config_save`
    worker_config_list = function() {
      rrq_worker_config_list(self)
    },

    ##' @description Return the value of a of worker configuration saved by
    ##' `$worker_config_save`
    ##'
    ##' @param name Name of the configuration
    worker_config_read = function(name) {
      rrq_worker_config_read(name, self)
    },

    ##' Report on worker "load" (the number of workers being used over
    ##' time). Reruns an object of class `worker_load`, for which a
    ##' `mean` method exists (this method is a work in progress and the
    ##' interface may change).
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##'   all active workers are used.
    worker_load = function(worker_ids = NULL) {
      rrq_worker_load(worker_ids, self)
    },

    ##' @description Send a message to workers. Sending a message returns
    ##' a message id, which can be used to poll for a response with the
    ##' other `message_*` methods.
    ##'
    ##' @param command A command, such as `PING`, `PAUSE`; see the Messages
    ##' section of the Details for al messages.
    ##'
    ##' @param args Arguments to the command, if supported
    ##'
    ##' @param worker_ids Optional vector of worker ids to send the message
    ##'   to. If `NULL` then the message will be sent to all active workers.
    message_send = function(command, args = NULL, worker_ids = NULL) {
      rrq_message_send(command, args, worker_ids, self)
    },

    ##' @description Detect if a response is available for a message
    ##'
    ##' @param message_id The message id
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##'   all active workers are used (note that this may differ to the set
    ##'   of workers that the message was sent to!)
    ##'
    ##' @param named Logical, indicating if the return vector should be named
    message_has_response = function(message_id, worker_ids = NULL,
                                    named = TRUE) {
      rrq_message_has_response(message_id, worker_ids, named, self)
    },

    ##' @description Get response to messages, waiting until the
    ##' message has been responded to.
    ##'
    ##' @param message_id The message id
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##'   all active workers are used (note that this may differ to the set
    ##'   of workers that the message was sent to!)
    ##'
    ##' @param named Logical, indicating if the return value should be
    ##'   named by worker id.
    ##'
    ##' @param delete Logical, indicating if messages should be deleted
    ##'   after retrieval
    ##'
    ##' @param timeout Integer, representing seconds to wait until the
    ##'   response has been received. An error will be thrown if a
    ##'   response has not been received in this time.
    ##'
    ##' @param time_poll If `timeout` is greater
    ##'   than zero, this is the polling interval used between redis calls.
    ##'   Increasing this reduces network load but increases the time that
    ##'   may be waited for.
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    message_get_response = function(message_id, worker_ids = NULL, named = TRUE,
                                    delete = FALSE, timeout = 0,
                                    time_poll = 0.05, progress = NULL) {
      rrq_message_get_response(message_id, worker_ids, named, delete, timeout,
                               time_poll, progress, self)
    },

    ##' @description Return ids for messages with responses for a
    ##' particular worker.
    ##'
    ##' @param worker_id The worker id
    message_response_ids = function(worker_id) {
      rrq_message_response_ids(worker_id, self)
    },

    ##' @description Send a message and wait for responses.
    ##' This is a helper function around `message_send` and
    ##' `message_get_response`.
    ##'
    ##' @param command A command, such as `PING`, `PAUSE`; see the Messages
    ##' section of the Details for al messages.
    ##'
    ##' @param args Arguments to the command, if supported
    ##'
    ##' @param worker_ids Optional vector of worker ids to send the message
    ##'   to. If `NULL` then the message will be sent to all active workers.
    ##' @param named Logical, indicating if the return value should be
    ##'   named by worker id.
    ##'
    ##' @param delete Logical, indicating if messages should be deleted
    ##'   after retrieval
    ##'
    ##' @param timeout Integer, representing seconds to wait until the
    ##'   response has been received. An error will be thrown if a
    ##'   response has not been received in this time.
    ##'
    ##' @param time_poll If `timeout` is greater
    ##'   than zero, this is the polling interval used between redis calls.
    ##'   Increasing this reduces network load but increases the time that
    ##'   may be waited for.
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    message_send_and_wait = function(command, args = NULL, worker_ids = NULL,
                                     named = TRUE, delete = TRUE, timeout = 600,
                                     time_poll = 0.05, progress = NULL) {
      rrq_message_send_and_wait(command, args, worker_ids, named, delete,
                                timeout, time_poll, progress, self)
    }
  ),

  private = list(
    keys = NULL,
    timeout_task_wait = NULL,
    follow = NULL,
    scripts = NULL,
    store = NULL
  ))



task_submit <- function(con, keys, store, task_id, dat, queue,
                        separate_process, timeout, depends_on = NULL) {
  task_submit_n(con, keys, store, task_id, list(object_to_bin(dat)), NULL,
                queue, separate_process, timeout, depends_on)
}


task_submit_n <- function(con, keys, store, task_ids, dat, key_complete, queue,
                          separate_process, timeout, depends_on = NULL) {
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
    run_task_cleanup_failure(con, keys, store, task_ids, TASK_IMPOSSIBLE, NULL)
    incomplete <- response$status[response$status %in% TASK$terminal_fail]
    names(incomplete) <- depends_on[response$status %in% TASK$terminal_fail]
    stop(sprintf("Failed to queue as dependent tasks failed:\n%s",
                 paste0(paste0(names(incomplete), ": ", incomplete),
                        collapse = ", ")))
  }

  complete <- depends_on[response$status == TASK_COMPLETE]
  for (dep_id in complete) {
    queue_dependencies(con, keys, dep_id, task_ids)
  }

  task_ids
}


worker_log_tail_1 <- function(con, keys, worker_id, n = 1) {
  ## More intuitive `n` behaviour for "print all entries"; n of Inf
  if (identical(n, Inf)) {
    n <- 0
  }
  log_key <- rrq_key_worker_log(keys$queue_id, worker_id)
  log <- as.character(con$LRANGE(log_key, -n, -1))
  worker_log_parse(log, worker_id)
}


worker_log_parse <- function(log, worker_id) {
  re <- "^([0-9.]+)(/[0-9]+)? ([^ ]+) ?(.*)$"
  if (!all(grepl(re, log))) {
    stop("Corrupt log")
  }
  time <- as.numeric(sub(re, "\\1", log))
  child <- as.integer(sub("/", "", sub(re, "\\2", log)))
  command <- sub(re, "\\3", log)
  message <- lstrip(sub(re, "\\4", log))
  data_frame(worker_id, child, time, command, message)
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


tasks_wait <- function(con, keys, store, task_ids, timeout, time_poll,
                       progress, key_complete, error, follow, single,
                       call = NULL) {
  ## This can be relaxed in recent Redis >= 6.0.0 as we then interpret
  ## time_poll as a double. To do this efficiently we'll want to get
  ## the version information stored into the redux client, which is
  ## not hard as we already do some negotiation
  assert_integer_like(time_poll)
  if (time_poll < 1L) {
    cli::cli_abort("'time_poll' cannot be less than 1", call = call)
  }
  if (follow) {
    task_ids_from <- task_follow(con, keys, task_ids)
  } else {
    task_ids_from <- task_ids
  }

  done <- set_names(
    hash_exists(con, keys$task_result, task_ids_from, TRUE),
    task_ids_from)

  if (is.null(key_complete)) {
    key_complete <- rrq_key_task_complete(keys$queue_id, task_ids_from)
    fetch <- function() {
      tmp <- con$BLPOP(key_complete[!done], time_poll)
      if (!is.null(tmp)) {
        done[[tmp[[2L]]]] <<- TRUE
      }
      done
    }
  } else {
    fetch <- function() {
      tmp <- con$BLPOP(key_complete, time_poll)
      if (!is.null(tmp)) {
        done[[tmp[[2L]]]] <<- TRUE
      }
      done
    }
  }

  if (!all(done)) {
    general_poll(fetch, 0, timeout, "tasks", TRUE, progress)
  }

  ## A bit inefficient, but we won't do it this way for long:
  controller <- rrq_controller2(keys$queue_id, con)
  if (single) {
    rrq_task_result(task_ids_from, error = error, controller = controller)
  } else {
    rrq_task_results(task_ids_from, error = error, controller = controller)
  }
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
    dependencies_exist <- rrq_task_exists(depends_on, controller = controller)
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

throw_task_errors <- function(res, single) {
  if (single) {
    stopifnot(length(res) == 1)
    if (inherits(res[[1]], "rrq_task_error")) {
      stop(res[[1]])
    }
  } else {
    is_error <- vlapply(res, inherits, "rrq_task_error")
    if (any(is_error)) {
      stop(rrq_task_error_group(unname(res[is_error]), length(res)))
    }
  }
}


## This is probably worth doing in lua, because we're going to hit
## this really very often.
task_follow <- function(con, keys, task_ids) {
  key <- keys$task_moved_to
  i <- rep_len(TRUE, length(task_ids))
  while (any(i)) {
    moved_to <- unname(from_redis_hash(con, key, task_ids[i]))
    is_terminal <- is.na(moved_to)
    i[is_terminal] <- FALSE
    task_ids[i] <- moved_to[!is_terminal]
  }
  task_ids
}


task_follow_root <- function(con, keys, task_ids) {
  task_ids_root <- unname(from_redis_hash(con, keys$task_moved_root, task_ids))
  task_ids_root[is.na(task_ids_root)] <- task_ids[is.na(task_ids_root)]
  task_ids_root
}


task_follow_chain <- function(con, keys, task_ids) {
  task_ids <- task_follow_root(con, keys, task_ids)
  chain <- NULL
  while (any(!is.na(task_ids))) {
    chain <- cbind(chain, task_ids, deparse.level = 0)
    task_ids <- unname(from_redis_hash(con, keys$task_moved_to, task_ids))
  }
  lapply(seq_len(nrow(chain)), function(i) na_drop(chain[i, ]))
}


is_task_redirect <- function(x) {
  is.character(x)
}


task_depends_down <- function(con, keys, task_ids) {
  key <- function(k) rrq_key_task_depends_down(keys$queue_id, k)
  task_depends_walk(con, key, task_ids)
}


task_depends_up <- function(con, keys, task_ids) {
  key <- function(k) rrq_key_task_depends_up_original(keys$queue_id, k)
  task_depends_walk(con, key, task_ids)
}


task_depends_walk <- function(con, key, task_ids) {
  ret <- list()
  while (length(task_ids) > 0) {
    deps <- lapply(
      con$pipeline(.commands = lapply(key(task_ids), redis$SMEMBERS)),
      list_to_character)
    i <- lengths(deps) > 0
    ret <- c(ret, set_names(deps[i], task_ids[i]))
    task_ids <- unique(unlist(deps[i]))
  }
  if (length(ret) == 0) NULL else ret
}
