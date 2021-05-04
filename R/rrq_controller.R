##' A queue controller.  Use this to interact with a queue/cluster.
##'
##' @title rrq queue controller
##'
##' @param queue_id An identifier for the queue.  This will prefix all
##'   keys in redis, so a prefix might be useful here depending on
##'   your use case (e.g. \code{rrq:<user>:<id>})
##'
##' @param con A redis connection
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
##' @export
rrq_controller <- function(queue_id, con = redux::hiredis()) {
  assert_scalar_character(queue_id)
  assert_is(con, "redis_api")
  rrq_controller_$new(queue_id, con)
}

##' @rdname rrq_controller
rrq_controller_ <- R6::R6Class(
  "rrq_controller",
  cloneable = FALSE,

  public = list(
    ##' @field con The redis connection
    con = NULL,

    ##' @field queue_id The queue id used on creation
    queue_id = NULL,

    ##' @field keys Internally used keys
    keys = NULL,

    ##' @field db Internally used storr database
    db = NULL,

    ##' @description Constructor (called by `rrq_controller()`)
    ##' @param queue_id An identifier for the queue
    ##' @param con A redis connection
    initialize = function(queue_id, con) {
      self$con <- con
      self$queue_id <- queue_id
      self$keys <- rrq_keys(self$queue_id)
      self$worker_config_save("localhost", overwrite = FALSE)
      self$db <- rrq_db(self$con, self$keys)
      private$scripts <- rrq_scripts_load(self$con)
      info <- object_to_bin(controller_info())
      rpush_max_length(self$con, self$keys$controller, info, 10)
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
    ##' @param worker_stop_timeout A timeout to pass to the worker if
    ##'   using `type = "message"`
    destroy = function(delete = TRUE, worker_stop_type = "message",
                       worker_stop_timeout = 0) {
      if (!is.null(self$con)) {
        rrq_clean(self$con, self$queue_id, delete, worker_stop_type,
                  worker_stop_timeout)
        ## render the controller useless:
        self$con <- NULL
        self$queue_id <- NULL
        self$keys <- NULL
      }
    },

    ##' @description Register a function to create an environment when
    ##'   creating a worker. When a worker starts, they will run this
    ##'   function.
    ##'
    ##' @param create A function that will create an environment. It will
    ##'   be called with no parameters, in a fresh R session.
    ##'
    ##' @param notify Boolean, indicating if we should send a `REFRESH`
    ##'   message to all workers to update their environment.
    envir = function(create, notify = TRUE) {
      if (is.null(create)) {
        self$con$DEL(self$keys$envir)
      } else {
        assert_is(create, "function")
        self$con$SET(self$keys$envir, object_to_bin(create))
      }
      if (notify) {
        self$message_send("REFRESH")
      }
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
    ##' @param key_complete The name of a Redis key to write to once the
    ##'   task is complete. You can use this with `$task_wait` to efficiently
    ##'   wait for the task to complete (i.e., without using a busy loop).
    ##'
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
    ##'   clean, subsequent runs are not affected by preceeding ones.
    ##'   The downside of this approach is a considerable overhead in
    ##'   starting the extenal process and transferring data back.
    ##'
    ##' @param timeout Optionally, a maximum allowed running time, in
    ##'   seconds. This parameter only has an effect if `separate_process`
    ##'   is `TRUE`. If given, then if the task takes longer than this
    ##'   time it will be stopped and the task status set to `TIMEOUT`.
    ##'
    ##' @param at_front Logical, if TRUE then add the task to the front
    ##'   of the queue.
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue.
    enqueue = function(expr, envir = parent.frame(), key_complete = NULL,
                       queue = NULL, separate_process = FALSE, timeout = NULL,
                       at_front = FALSE, depends_on = NULL) {
      self$enqueue_(substitute(expr), envir, key_complete, queue,
                    separate_process, timeout, at_front, depends_on)
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
    ##' @param key_complete The name of a Redis key to write to once the
    ##'   task is complete. You can use this in conjunction with something
    ##'   like `BLPOP` to wait until a task is complete without a busy (sleep)
    ##'   loop.
    ##'
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
    ##' @param timeout Optionally, a maximum allowed running time, in
    ##'   seconds (see `$enqueue` for details).
    ##'
    ##' @param at_front Logical, if TRUE then add the task to the front
    ##'   of the queue.
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue.
    enqueue_ = function(expr, envir = parent.frame(), key_complete = NULL,
                        queue = NULL, separate_process = FALSE, timeout = NULL,
                        at_front = FALSE, depends_on = NULL) {
      dat <- expression_prepare(expr, envir, NULL, self$db)
      verify_dependencies_exist(self, depends_on)
      task_submit(self$con, self$keys, dat, key_complete, queue,
                  separate_process, timeout, at_front, depends_on)
    },

    ##' @description Apply a function over a list of data. This is
    ##' equivalent to using `$enqueue()` over each element in the list.
    ##'
    ##' @param x A list of data to apply our function against
    ##'
    ##' @param fun A function to be applied to each element of `x`
    ##'
    ##' @param ... Additional arguments to `fun`
    ##'
    ##' @param dots As an alternative to `...`, you can provide the dots
    ##'   as a list of additional arguments. This may be easier to program
    ##'   against.
    ##'
    ##' @param envir The environment to use to try and find the function
    ##'
    ##' @param collect_timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if all tasks have not completed. If given  as
    ##'   `0`, then we return a handle that can be used to check for tasks
    ##'   using `bulk_wait`
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion.
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
    ##' @param task_timeout Optionally, a maximum allowed running time, in
    ##'   seconds (see the `timeout` argument of `$enqueue` for details).
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue. Dependencies are applied to all
    ##'   tasks added to the queue.
    lapply = function(x, fun, ..., dots = NULL,
                      envir = parent.frame(), queue = NULL,
                      separate_process = FALSE, task_timeout = NULL,
                      depends_on = NULL,
                      collect_timeout = Inf, time_poll = NULL,
                      progress = NULL) {
      if (is.null(dots)) {
        dots <- as.list(substitute(list(...)))[-1L]
      }
      self$lapply_(x, substitute(fun), dots = dots, envir = envir,
                   queue = queue, separate_process = separate_process,
                   task_timeout = task_timeout,
                   depends_on = depends_on, collect_timeout = collect_timeout,
                   time_poll = time_poll, progress = NULL)
    },

    ##' @description The "standard evaluation" version of `$lapply()`.
    ##' This differs in how the function is found and how dots are passed.
    ##' With this version, both are passed by value; this may create more
    ##' overhead on the redis server as the values of the variables will
    ##' be copied over rather than using their names if possible.
    ##'
    ##' @param x A list of data to apply our function against
    ##'
    ##' @param fun A function to be applied to each element of `x`
    ##'
    ##' @param ... Additional arguments to `fun`
    ##'
    ##' @param dots As an alternative to `...`, you can provide the dots
    ##'   as a list of additional arguments. This may be easier to program
    ##'   against.
    ##'
    ##' @param envir The environment to use to try and find the function
    ##'
    ##' @param collect_timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if all tasks have not completed. If given  as
    ##'   `0`, then we return a handle that can be used to check for tasks
    ##'   using `bulk_wait`
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion.
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
    ##' @param task_timeout Optionally, a maximum allowed running time, in
    ##'   seconds (see the `timeout` argument of `$enqueue` for details).
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue. Dependencies are applied to all
    ##'   tasks added to the queue.
    lapply_ = function(x, fun, ..., dots = NULL,
                       envir = parent.frame(), queue = NULL,
                       separate_process = FALSE, task_timeout = NULL,
                       depends_on = NULL, collect_timeout = Inf,
                       time_poll = NULL, progress = NULL) {
      if (is.null(dots)) {
        dots <- list(...)
      }
      rrq_lapply(self$con, self$keys, self$db, x, fun, dots, envir, queue,
                 separate_process, task_timeout, depends_on,
                 collect_timeout, time_poll, progress)
    },

    ##' @description Send a bulk set of tasks to your workers.
    ##' This function is a bit like a mash-up of [Map] and [do.call],
    ##' when used with a [data.frame] argument, which is typically what
    ##' is provided. Rather than `$lapply()` which applies `fun` to each
    ##' element of `x`, `enqueue_bulk will apply over each row of `x`,
    ##' spreading the columms out as arguments. If you have a function
    ##' `f(a, b)` and a [data.frame] with columns `a` and `b` this
    ##' should feel intuitive.
    ##'
    ##' @param x Typically a [data.frame], which you want to apply `fun`
    ##'   over, row-wise. The names of the `data.frame` must match the
    ##'   arguments of your function.
    ##'
    ##' @param fun A function
    ##'
    ##' @param ... Additional arguments to add to every call to `fun`
    ##'
    ##' @param dots As an alternative to `...`, you can provide the dots
    ##'   as a list of additional arguments. This may be easier to program
    ##'   against.
    ##'
    ##' @param envir The environment to use to try and find the function
    ##'
    ##' @param collect_timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if all tasks have not completed. If given  as
    ##'   `0`, then we return a handle that can be used to check for tasks
    ##'   using `bulk_wait`
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion.
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
    ##' @param task_timeout Optionally, a maximum allowed running time, in
    ##'   seconds (see the `timeout` argument of `$enqueue` for details).
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue. Dependencies are applied to all
    ##'   tasks added to the queue.
    enqueue_bulk = function(x, fun, ..., dots = NULL,
                            envir = parent.frame(), queue = NULL,
                            separate_process = FALSE, task_timeout = NULL,
                            depends_on = NULL, collect_timeout = Inf,
                            time_poll = NULL, progress = NULL) {
      if (is.null(dots)) {
        dots <- as.list(substitute(list(...)))[-1L]
      }
      self$enqueue_bulk_(x, substitute(fun), dots = dots, envir = envir,
                         queue = queue, separate_process = separate_process,
                         task_timeout = task_timeout, depends_on = depends_on,
                         collect_timeout = collect_timeout,
                         time_poll = time_poll, progress = progress)
    },

    ##' @description The "standard evaluation" version of `$enqueue_bulk()`.
    ##' This differs in how the function is found and how dots are passed.
    ##' With this version, both are passed by value; this may create more
    ##' overhead on the redis server as the values of the variables will
    ##' be copied over rather than using their names if possible.
    ##'
    ##' @param x Typically a [data.frame], which you want to apply `fun`
    ##'   over, row-wise. The names of the `data.frame` must match the
    ##'   arguments of your function.
    ##'
    ##' @param fun A function
    ##'
    ##' @param ... Additional arguments to add to every call to `fun`
    ##'
    ##' @param dots As an alternative to `...`, you can provide the dots
    ##'   as a list of additional arguments. This may be easier to program
    ##'   against.
    ##'
    ##' @param envir The environment to use to try and find the function
    ##'
    ##' @param collect_timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if all tasks have not completed. If given  as
    ##'   `0`, then we return a handle that can be used to check for tasks
    ##'   using `bulk_wait`
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion.
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
    ##' @param task_timeout Optionally, a maximum allowed running time, in
    ##'   seconds (see the `timeout` argument of `$enqueue` for details).
    ##'
    ##' @param depends_on Vector or list of IDs of tasks which must have
    ##'   completed before this job can be run. Once all dependent tasks
    ##'   have been successfully run, this task will get added to the
    ##'   queue. If the dependent task fails then this task will be
    ##'   removed from the queue. Dependencies are applied to all
    ##'   tasks added to the queue.
    enqueue_bulk_ = function(x, fun, ..., dots = NULL,
                             envir = parent.frame(), queue = NULL,
                             separate_process = FALSE, task_timeout = NULL,
                             depends_on = NULL, collect_timeout = Inf,
                             time_poll = NULL, progress = NULL) {
      if (is.null(dots)) {
        dots <- list(...)
      }
      rrq_enqueue_bulk(self$con, self$keys, self$db, x, fun, dots,
                       envir, queue, separate_process, task_timeout, depends_on,
                       collect_timeout, time_poll, progress)
    },

    ##' @description Wait for a group of tasks
    ##'
    ##' @param x An object of class `rrq_bulk`, as created by `$lapply()`
    ##'
    ##' @param timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if the task has not completed.
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion.
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    bulk_wait = function(x, timeout = Inf, time_poll = NULL,
                         progress = NULL) {
      rrq_bulk_wait(self$con, self$keys, x, timeout, time_poll, progress)
    },

    ##' @description List ids of all tasks known to this rrq controller
    task_list = function() {
      as.character(self$con$HKEYS(self$keys$task_expr))
    },

    ##' @description Test if task with id `task_ids` is known to this
    ##'   rrq controller
    ##' @param task_ids Character vector of task ids to check for existence.
    task_exists = function(task_ids = NULL) {
      exists <- !vlapply(self$con$HMGET(self$keys$task_expr, task_ids), is.null)
      setNames(exists, task_ids)
    },

    ##' @description Return a character vector of task statuses. The name
    ##' of each element corresponds to a task id, and the value will be
    ##' one of the possible statuses ("PENDING", "COMPLETE", etc).
    ##'
    ##' @param task_ids Optional character vector of task ids for which you
    ##' would like statuses. If not given (or `NULL`) then the status of
    ##' all task ids known to this rrq controller is returned.
    task_status = function(task_ids = NULL) {
      task_status(self$con, self$keys, task_ids)
    },

    ##' @description Retrieve task progress, if set. This will be `NULL`
    ##'   if progress has never been registered, otherwise whatever value
    ##'   was set - can be an arbitrary R object.
    ##'
    ##' @param task_id A single task id for which the progress is wanted.
    task_progress = function(task_id) {
      task_progress(self$con, self$keys, task_id)
    },

    ##' @description Provide a high level overview of task statuses
    ##' for a set of task ids, being the count in major categories of
    ##' `PENDING`, `RUNNING`, `COMPLETE` and `ERROR`.
    ##'
    ##' @param task_ids Optional character vector of task ids for which you
    ##' would like the overview. If not given (or `NULL`) then the status of
    ##' all task ids known to this rrq controller is used.
    task_overview = function(task_ids = NULL) {
      task_overview(self$con, self$keys, task_ids)
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
    task_position = function(task_ids, missing = 0L, queue = NULL) {
      task_position(self$con, self$keys, task_ids, missing, queue)
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
    task_preceeding = function(task_id, queue = NULL) {
      task_preceeding(self$con, self$keys, task_id, queue)
    },

    ##' @description Get the result for a single task (see `$tasks_result`
    ##'   for a method for efficiently getting multiple results at once).
    ##'   Returns the value of running the task if it is complete, and an
    ##'   error otherwise.
    ##'
    ##' @param task_id The single id for which the result is wanted.
    task_result = function(task_id) {
      assert_scalar_character(task_id)
      self$tasks_result(task_id)[[1L]]
    },

    ##' @description Get the results of a group of tasks, returning them as a
    ##' list.
    ##'
    ##' @param task_ids A vector of task ids for which the task result
    ##' is wanted.
    tasks_result = function(task_ids) {
      task_results(self$con, self$keys, task_ids)
    },

    ##' @description Poll for a task to complete, returning the result
    ##' when completed. If the task has already completed this is
    ##' roughly equivalent to `task_result`. See `$tasks_wait` for an
    ##' efficient way of doing this for a group of tasks.
    ##'
    ##' @param task_id The single id that we will wait for
    ##'
    ##' @param timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if the task has not completed.
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion. The default and behaviour depend on `key_complete` -
    ##'   see the details section.
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param key_complete Optional key used when `enqueing` the tasks
    ##'   that will be written to on completion.
    ##'
    ##' @details The polling behaviour depends on `key_complete`. If
    ##' `key_complete` is `NULL` then we use a busy-loop, sleeping for
    ##' `time_poll` seconds between polls of Redis. As such, the smaller
    ##' the `time_poll` the faster you will get your result, as it may
    ##' be delayed by up to `time_poll`, but the heavier the network and
    ##' Redis load (the default is 0.05s). Alternatively, if
    ##' `key_complete` is given then your task will be returned as soon
    ##' as it is written into Redis, and `time_poll` is the time between
    ##' subsequent calls to the Redis function that enables
    ##' this. Shorter values of `time_poll` then make R more responsive
    ##' to being interrupted and longer values reduce network load (the
    ##' default is 1s)
    task_wait = function(task_id, timeout = Inf, time_poll = NULL,
                       progress = NULL, key_complete = NULL) {
      assert_scalar_character(task_id)
      self$tasks_wait(task_id, timeout, time_poll,
                      progress, key_complete)[[1L]]
    },

    ##' @description Poll for a group of tasks to complete, returning the
    ##' result as list when completed. If the tasks have already completed
    ##' this is roughly equivalent to `tasks_result`.
    ##'
    ##' @param task_ids A vector of task ids to poll for
    ##'
    ##' @param timeout Optional timeout, in seconds, after which an
    ##'   error will be thrown if the task has not completed.
    ##'
    ##' @param time_poll Optional time with which to "poll" for
    ##'   completion. The default and behaviour depend on `key_complete` -
    ##'   see the details section of `$task_wait`
    ##'
    ##' @param progress Optional logical indicating if a progress bar
    ##'   should be displayed. If `NULL` we fall back on the value of the
    ##'   global option `rrq.progress`, and if that is unset display a
    ##'   progress bar if in an interactive session.
    ##'
    ##' @param key_complete Optional key used when `enqueing` the tasks
    ##'   that will be written to on completion. If used, then all tasks
    ##'   must share the same completion key.
    tasks_wait = function(task_ids, timeout = Inf, time_poll = NULL,
                          progress = NULL, key_complete = NULL) {
      tasks_wait(self$con, self$keys, task_ids, timeout, time_poll, progress,
                 key_complete)
    },

    ##' @description Delete one or more tasks
    ##'
    ##' @param task_ids Vector of task ids to delete
    ##'
    ##' @param check Logical indicating if we should check that the tasks
    ##'   are not running. Deleting running tasks is unlikely to result in
    ##'   desirable behaviour.
    task_delete = function(task_ids, check = TRUE) {
      task_delete(self$con, self$keys, task_ids, check)
    },

    ##' @description Cancel a single task. If the task is `PENDING` it
    ##' will be deleted. If `RUNNING` then the task will be stopped if
    ##' it was set to run in a separate process (i.e., queued with
    ##' `separate_process = TRUE`). Dependent tasks will be marked as
    ##' impossible.
    ##'
    ##' @param task_id Id of the task to cancel
    ##'
    ##' @param wait Wait for the task to be stopped, if it was running. If
    ##'   `delete` is `TRUE`, then we will always wait for the task to stop.
    ##'
    ##' @param delete Delete the task after cancelling (if cancelling
    ##'   was successful).
    ##'
    ##' @return Nothing if successfully cancelled, otherwise throws an
    ##' error with task_id and status e.g. Task 123 is not running (MISSING)
    task_cancel = function(task_id, wait = TRUE, delete = TRUE) {
      task_cancel(self$con, self$keys, private$scripts, task_id, wait, delete)
    },

    ##' @description Fetch internal data about a task from Redis
    ##' (expert use only).
    ##'
    ##' @param task_id The id of the task
    task_data = function(task_id) {
      task_data(self$con, self$keys, self$db, task_id)
    },

    ##' @description Returns the number of tasks in the queue (waiting for
    ##' an available worker).
    ##'
    ##' @param queue The name of the queue to query (defaults to the
    ##'   "default" queue).
    queue_length = function(queue = NULL) {
      self$con$LLEN(rrq_key_queue(self$keys$queue_id, queue))
    },

    ##' @description Returns the keys in the task queue.
    ##'
    ##' @param queue The name of the queue to query (defaults to the
    ##'   "default" queue).
    queue_list = function(queue = NULL) {
      key_queue <- rrq_key_queue(self$keys$queue_id, queue)
      list_to_character(self$con$LRANGE(key_queue, 0, -1))
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
      queue_remove(self$con, self$keys, task_ids, queue %||% QUEUE_DEFAULT)
    },

    ##' @description Return deferred tasks and what they are waiting on.
    ##'   Note this is in an arbitrary order, tasks will be added to the
    ##'   queue as their dependencies are satisfied.
    deferred_list = function() {
      deferred_list(self$con, self$keys)
    },

    ##' @description Returns the number of active workers
    worker_len = function() {
      worker_len(self$con, self$keys)
    },

    ##' @description Returns the ids of active workers
    worker_list = function() {
      worker_list(self$con, self$keys)
    },

    ##' @description Returns the ids of workers known to have exited
    worker_list_exited = function() {
      worker_list_exited(self$con, self$keys)
    },

    ##' @description Returns a list of information about active
    ##' workers (or exited workers if `worker_ids` includes them).
    ##
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' all active workers are used.
    worker_info = function(worker_ids = NULL) {
      worker_info(self$con, self$keys, worker_ids)
    },

    ##' @description Returns a character vector of current worker statuses
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' all active workers are used.
    worker_status = function(worker_ids = NULL) {
      worker_status(self$con, self$keys, worker_ids)
    },

    ##' @description Returns the last (few) elements in the worker
    ##' log. The log will be returned as a [data.frame] of entries
    ##' `worker_id` (the worker id), `time` (the time in Redis when the
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
      worker_log_tail(self$con, self$keys, worker_ids, n)
    },

    ##' @description Returns the task id that each worker is working on,
    ##' if any.
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' all active workers are used.
    worker_task_id = function(worker_ids = NULL) {
      worker_task_id(self$con, self$keys, worker_ids)
    },

    ##' @description Cleans up workers known to have exited
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##' rrq looks for exited workers.
    worker_delete_exited = function(worker_ids = NULL) {
      worker_delete_exited(self$con, self$keys, worker_ids)
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
    ##'   is `message`).
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
                           timeout = 0, time_poll = 1, progress = NULL) {
      worker_stop(self$con, self$keys, worker_ids, type,
                   timeout, time_poll, progress)
    },

    ##' @description Detects exited workers through a lapsed heartbeat
    worker_detect_exited = function() {
      worker_detect_exited(self)
    },

    ##' @description Return the contents of a worker's process log, if
    ##' it is located on the same physical storage (including network
    ##' storage) as the controller. This will generally behave for
    ##' workers started with [worker_spawn] but may require significant
    ##' care otherwise.
    ##'
    ##' @param worker_id The worker for which the log is required
    worker_process_log = function(worker_id) {
      assert_scalar(worker_id)
      path <- self$con$HGET(self$keys$worker_process, worker_id)
      if (is.null(path)) {
        stop("Process log not enabled for this worker")
      }
      readLines(path)
    },

    ##' @description Save a worker configuration, which can be used to
    ##' start workers with a set of options with the cli. These
    ##' correspond to arguments to [rrq::rrq_worker].
    ##'
    ##' @param name Name for this configuration
    ##'
    ##' @param time_poll Poll time.  Longer values here will reduce the
    ##'   impact on the database but make workers less responsive to being
    ##'   killed with an interrupt.  The default should be good for most
    ##'   uses, but shorter values are used for debugging.
    ##'
    ##' @param timeout Optional timeout to set for the worker.  This is
    ##'   (roughly) equivalent to issuing a \code{TIMEOUT_SET} message
    ##'   after initialising the worker, except that it's guaranteed to be
    ##'   run by all workers.
    ##'
    ##' @param queue Optional character vector of queues to listen on
    ##'   for jobs. There is a default queue which is always listened
    ##'   on (called 'default'). You can specify additional names here
    ##'   and jobs put onto these queues with `$enqueue()` will have
    ##'   *higher* priority than the default. You can explicitly list
    ##'   the "default" queue (e.g., `queue = c("high", "default",
    ##'   "low")`) to set the position of the default queue.
    ##'
    ##' @param heartbeat_period Optional period for the heartbeat.  If
    ##'   non-NULL then a heartbeat process will be started (using
    ##' [`rrq::heartbeat`] which can be used to build fault tolerant queues.
    ##'
    ##' @param verbose Logical, indicating if the worker should print
    ##'   logging output to the screen.  Logging to screen has a small but
    ##'   measurable performance cost, and if you will not collect system
    ##'   logs from the worker then it is wasted time.  Logging to the
    ##'   redis server is always enabled.
    ##'
    ##' @param overwrite Logical, indicating if an existing configuration
    ##'   with this `name` should be overwritten if it exists (if
    ##'   `overwrite = FALSE` and the configuration exists an error will
    ##'   be thrown).
    worker_config_save = function(name, time_poll = NULL, timeout = NULL,
                                  queue = NULL, heartbeat_period = NULL,
                                  verbose = NULL, overwrite = TRUE) {
      worker_config_save(self$con, self$keys, name, time_poll, timeout, queue,
                         heartbeat_period, verbose, overwrite)
    },

    ##' @description Return names of worker configurations saved by
    ##' `$worker_config_save`
    worker_config_list = function() {
      list_to_character(self$con$HKEYS(self$keys$worker_config))
    },

    ##' @description Return the value of a of worker configuration saved by
    ##' `$worker_config_save`
    ##'
    ##' @param name Name of the configuration
    worker_config_read = function(name) {
      worker_config_read(self$con, self$keys, name)
    },

    ##' Report on worker "load" (the number of workers being used over
    ##' time). Reruns an object of class `worker_load`, for which a
    ##' `mean` method exists (this method is a work in progress and the
    ##' interface may change).
    ##'
    ##' @param worker_ids Optional vector of worker ids. If `NULL` then
    ##'   all active workers are used.
    worker_load = function(worker_ids = NULL) {
      worker_load(self$con, self$keys, worker_ids)
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
      message_send(self$con, self$keys, command, args, worker_ids)
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
      message_has_response(self$con, self$keys, message_id, worker_ids, named)
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
                                    time_poll = 1, progress = NULL) {
      message_get_response(self$con, self$keys, message_id, worker_ids, named,
                           delete, timeout, time_poll, progress)
    },

    ##' @description Return ids for messages with responses for a
    ##' particular worker.
    ##'
    ##' @param worker_id The worker id
    message_response_ids = function(worker_id) {
      message_response_ids(self$con, self$keys, worker_id)
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
                                     time_poll = 1, progress = NULL) {
      message_send_and_wait(self$con, self$keys, command, args, worker_ids,
                            named, delete, timeout, time_poll, progress)
    }
  ),

  private = list(
    scripts = NULL
  ))

task_status <- function(con, keys, task_ids) {
  from_redis_hash(con, keys$task_status, task_ids, missing = TASK_MISSING)
}


task_progress <- function(con, keys, task_id) {
  assert_scalar_character(task_id)
  ret <- con$HGET(keys$task_progress, task_id)
  if (!is.null(ret)) {
    ret <- bin_to_object(ret)
  }
  ret
}


task_overview <- function(con, keys, task_ids) {
  lvls <- c(TASK_PENDING, TASK_RUNNING, TASK_COMPLETE, TASK_ERROR)
  status <- task_status(con, keys, task_ids)
  lvls <- c(lvls, setdiff(unique(status), lvls))
  as.list(table(factor(status, lvls)))
}

## NOTE: This is not crazy efficient; we pull the entire list down
## which is not ideal.  However, in practice it seems fairly fast.
## But one should be careful to adjust the polling interval of
## something usnig this not to flood the server with excessive load.
##
## A better way would possibly be to use a LUA script; especially for
## the case where there is a single job that'd be fairly easy to do.
task_position <- function(con, keys, task_ids, missing, queue) {
  key_queue <- rrq_key_queue(keys$queue_id, queue)
  queue_contents <- vcapply(con$LRANGE(key_queue, 0, -1L), identity)
  match(task_ids, queue_contents, missing)
}

task_preceeding <- function(con, keys, task_id, queue) {
  key_queue <- rrq_key_queue(keys$queue_id, queue)
  queue_contents <- vcapply(con$LRANGE(key_queue, 0, -1L), identity)
  task_position <- match(task_id, queue_contents)
  if (is.na(task_position)) {
    return(NULL)
  }
  queue_contents[seq_len(task_position - 1)]
}

task_submit <- function(con, keys, dat, key_complete, queue,
                        separate_process, timeout, at_front = FALSE,
                        depends_on = NULL) {
  task_submit_n(con, keys, list(object_to_bin(dat)), key_complete, queue,
                separate_process, timeout, at_front, depends_on)
}

task_delete <- function(con, keys, task_ids, check = TRUE) {
  if (check) {
    st <- from_redis_hash(con, keys$task_status, task_ids,
                          missing = TASK_MISSING)
    if (any(st == TASK_RUNNING)) {
      stop("Can't delete running tasks")
    }
  }

  original_deps_keys <- rrq_key_task_dependencies_original(
    keys$queue_id, task_ids)
  dependency_keys <- rrq_key_task_dependencies(keys$queue_id, task_ids)
  dependent_keys <- rrq_key_task_dependents(keys$queue_id, task_ids)
  res <- con$pipeline(.commands = c(
    lapply(task_ids, function(x) redis$HGET(keys$task_status, x)),
    set_names(lapply(dependent_keys, redis$SMEMBERS), task_ids),
    list(
      redis$HDEL(keys$task_expr,     task_ids),
      redis$HDEL(keys$task_status,   task_ids),
      redis$HDEL(keys$task_result,   task_ids),
      redis$HDEL(keys$task_complete, task_ids),
      redis$HDEL(keys$task_progress, task_ids),
      redis$HDEL(keys$task_worker,   task_ids),
      redis$HDEL(keys$task_local,    task_ids),
      redis$SREM(keys$deferred_set,  task_ids)
    ),
    lapply(original_deps_keys, redis$DEL),
    lapply(dependency_keys, redis$DEL),
    lapply(dependent_keys, redis$DEL)
    ))
  queue <- list_to_character(con$HMGET(keys$task_queue, task_ids))
  queue_remove(con, keys, task_ids, queue)

  ## We only want to cancel dependencies i.e. set status to IMPOSSIBLE when
  ## A. They are dependents of a task which is PENDING or DEFERRED AND
  ## B. Their dependencies have not already been deleted or set to ERRORED, etc.
  ## i.e. their dependencies are also DEFERRED
  status <- res[seq_along(task_ids)]
  ids_to_cancel <- task_ids[unlist(status) %in% c(TASK_PENDING, TASK_DEFERRED)]
  dependents <- unique(unlist(res[ids_to_cancel]))
  if (length(dependents) > 0) {
    status_dependent <- con$HMGET(keys$task_status, dependents)
    cancel <- dependents[status_dependent == TASK_DEFERRED]
    if (length(cancel) > 0) {
      con$pipeline(
        redis$HMSET(keys$task_status, cancel,
                    rep_len(TASK_IMPOSSIBLE, length(cancel))),
        redis$SREM(keys$deferred_set, cancel)
      )
      cancel_dependencies(con, keys, cancel)
    }
  }

  invisible()
}

task_cancel <- function(con, keys, scripts, task_id, wait = FALSE,
                        delete = TRUE) {
  ## There are several steps here, which will all be executed in one
  ## block which removes the possibility of race conditions:
  ##
  ## * Remove the task_id from its queue (whichever it is in) so that
  ##   it cannot be picked up by any worker (prevents status moving
  ##   from PENDING -> RUNNING)
  ##
  ## * Mark the job as cancelled so that if it is running on a
  ##   separate process it will be eligible to be stopped as soon as
  ##   possible.
  ##
  ## * Determine if it is a local or a separate process task so we
  ## * know if it will be cancelled if it was running.
  ##
  ## * Retrieve the status so that we know the task status before any
  ##   change can happen.
  dat <- con$pipeline(
    dropped = redis$EVALSHA(scripts$queue_delete, 1L, keys$task_queue, task_id),
    cancel = redis$HSET(keys$task_cancel, task_id, "TRUE"),
    status = redis$HGET(keys$task_status, task_id),
    local = redis$HGET(keys$task_local, task_id))

  task_status <- dat$status %||% TASK_MISSING

  if (!(task_status %in% c(TASK_PENDING, TASK_DEFERRED, TASK_RUNNING))) {
    stop(sprintf("Task %s is not cancelable (%s)", task_id, task_status))
  }

  cancel_dependencies(con, keys, task_id)

  if (task_status == TASK_RUNNING) {
    if (dat$local != "FALSE") {
      stop(sprintf(
        "Can't cancel running task '%s' as not in separate process", task_id))
    }
    if (delete || wait) {
      timeout_wait <- 10
      wait_status_change(con, keys, task_id, TASK_RUNNING, timeout_wait)
    }
  }

  if (delete) {
    task_delete(con, keys, task_id, FALSE)
  }

  invisible(NULL)
}


task_data <- function(con, keys, db, task_id) {
  expr <- con$HGET(keys$task_expr, task_id)
  if (is.null(expr)) {
    stop(sprintf("Task '%s' not found", task_id))
  }
  task <- bin_to_object(expr)
  data <- as.list(expression_restore_locals(task, emptyenv(), db))
  task$objects <- data[names(task$objects)]
  task
}


task_submit_n <- function(con, keys, dat, key_complete, queue,
                          separate_process, timeout, at_front = FALSE,
                          depends_on = NULL) {
  n <- length(dat)
  task_ids <- ids::random_id(n)
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

  original_deps_keys <- rrq_key_task_dependencies_original(
    keys$queue_id, task_ids)
  dependency_keys <- rrq_key_task_dependencies(keys$queue_id, task_ids)
  dependent_keys <- rrq_key_task_dependents(keys$queue_id, depends_on)

  if (!is.null(key_complete)) {
    cmds <- list(
      redis$HMSET(keys$task_complete, task_ids, rep_len(key_complete, n)))
  } else {
    cmds <- list()
  }
  cmds <- c(
    cmds,
    list(
      redis$HMSET(keys$task_expr, task_ids, dat),
      redis$HMSET(keys$task_status, task_ids, rep_len(TASK_PENDING, n)),
      redis$HMSET(keys$task_queue, task_ids, rep_len(queue, n)),
      redis$HMSET(keys$task_local, task_ids, rep_len(local, n))),
    timeout)
  if (length(depends_on) > 0) {
    cmds <- c(
      cmds,
      list(
        status = redis$HMGET(keys$task_status, depends_on),
        redis$HMSET(keys$task_status, task_ids, rep_len(TASK_DEFERRED, n))),
      lapply(original_deps_keys, redis$SADD, depends_on),
      lapply(dependency_keys, redis$SADD, depends_on),
      lapply(dependent_keys, redis$SADD, task_ids),
      list(redis$SADD(keys$deferred_set, task_ids))
    )
  } else if (at_front) {
    cmds <- c(cmds, list(redis$LPUSH(key_queue, task_ids)))
  } else {
    cmds <- c(cmds, list(redis$RPUSH(key_queue, task_ids)))
  }
  response <- con$pipeline(.commands = cmds)

  ## If any dependencies will never be satisfied then cleanup and error
  incomplete_status <- c(TASK_ERROR, TASK_DIED, TASK_CANCELLED,
                         TASK_IMPOSSIBLE)
  if (any(response$status %in% incomplete_status)) {
    n <- length(task_ids)
    con$pipeline(
      redis$HMSET(keys$task_status, task_ids, rep_len(TASK_IMPOSSIBLE, n)),
      redis$SREM(keys$deferred_set, task_ids)
    )
    cancel_dependencies(con, keys, task_ids)
    incomplete <- response$status[response$status %in% incomplete_status]
    names(incomplete) <- depends_on[response$status %in% incomplete_status]
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


task_results <- function(con, keys, task_ids) {
  res <- from_redis_hash(con, keys$task_result, task_ids, identity, NULL)
  err <- lengths(res) == 0L
  if (any(err)) {
    stop("Missing some results")
  }
  lapply(res, bin_to_object)
}

worker_len <- function(con, keys) {
  con$SCARD(keys$worker_name)
}
worker_list <- function(con, keys) {
  worker_naturalsort(as.character(con$SMEMBERS(keys$worker_name)))
}

worker_list_exited <- function(con, keys) {
  setdiff(as.character(con$HKEYS(keys$worker_info)), worker_list(con, keys))
}

worker_status <- function(con, keys, worker_ids = NULL) {
  from_redis_hash(con, keys$worker_status, worker_ids)
}

worker_info <- function(con, keys, worker_ids = NULL) {
  from_redis_hash(con, keys$worker_info, worker_ids,
                  f = Vectorize(bin_to_object_safe, SIMPLIFY = FALSE))
}

worker_log_tail <- function(con, keys, worker_ids = NULL, n = 1) {
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }
  tmp <- lapply(worker_ids, function(i) worker_log_tail_1(con, keys, i, n))
  if (length(tmp) > 0L) {
    n <- viapply(tmp, nrow)
    ret <- do.call("rbind", tmp, quote = TRUE)
    ret <- ret[order(ret$time, ret$worker_id), ]
    rownames(ret) <- NULL
    ret
  } else {
    data_frame(worker_id = character(0),
               time = numeric(0),
               command = character(0),
               message = character(0))
  }
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
  re <- "^([0-9.]+) ([^ ]+) ?(.*)$"
  if (!all(grepl(re, log))) {
    stop("Corrupt log")
  }
  time <- as.numeric(sub(re, "\\1", log))
  command <- sub(re, "\\2", log)
  message <- lstrip(sub(re, "\\3", log))
  data.frame(worker_id, time, command, message, stringsAsFactors = FALSE)
}


worker_task_id <- function(con, keys, worker_id) {
  from_redis_hash(con, keys$worker_task, worker_id)
}

worker_delete_exited <- function(con, keys, worker_ids = NULL) {
  ## This only includes things that have been processed and had task
  ## orphaning completed.
  exited <- worker_list_exited(con, keys)
  if (is.null(worker_ids)) {
    worker_ids <- exited
  }
  extra <- setdiff(worker_ids, exited)
  if (length(extra)) {
    ## TODO: this whole thing can be improved because we might want to
    ## inform the user if the workers are not known.
    stop(sprintf("Workers %s may not have exited or may not exist",
                 paste(extra, collapse = ", ")))
  }

  if (length(worker_ids) > 0L) {
    con$HDEL(keys$worker_name,   worker_ids)
    con$HDEL(keys$worker_status, worker_ids)
    con$HDEL(keys$worker_task,   worker_ids)
    con$HDEL(keys$worker_info,   worker_ids)
    con$DEL(c(rrq_key_worker_log(keys$queue_id, worker_ids),
              rrq_key_worker_message(keys$queue_id, worker_ids),
              rrq_key_worker_response(keys$queue_id, worker_ids)))
  }
  worker_ids
}

worker_stop <- function(con, keys, worker_ids = NULL, type = "message",
                        timeout = 0, time_poll = 1, progress = NULL) {
  type <- match.arg(type, c("message", "kill", "kill_local"))
  if (is.null(worker_ids)) {
    worker_ids <- worker_list(con, keys)
  }
  if (length(worker_ids) == 0L) {
    return(invisible(worker_ids))
  }

  if (type == "message") {
    message_id <- message_send(con, keys, "STOP", worker_ids = worker_ids)
    if (timeout > 0L) {
      message_get_response(con, keys, message_id, worker_ids,
                           delete = FALSE, timeout = timeout,
                           time_poll = time_poll,
                           progress = progress)
    }
  } else if (type == "kill") {
    info <- worker_info(con, keys, worker_ids)
    heartbeat_key <- vcapply(info, function(x)
      x$heartbeat_key %||% NA_character_)
    if (any(is.na(heartbeat_key))) {
      stop("Worker does not support heatbeat - can't kill with signal: ",
           paste(worker_ids[is.na(heartbeat_key)], collapse = ", "))
    }
    for (key in heartbeat_key) {
      heartbeat_send_signal(con, key, tools::SIGTERM)
    }
  } else { # kill_local
    info <- worker_info(con, keys, worker_ids)
    is_local <- vcapply(info, "[[", "hostname") == hostname()
    if (!all(is_local)) {
      stop("Not all workers are local: ",
           paste(worker_ids[!is_local], collapse = ", "))
    }
    tools::pskill(vnapply(info, "[[", "pid"), tools::SIGTERM)
  }
  invisible(worker_ids)
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

## This is very much a beginning here; it might be nicer to be able to
## do this for a given time interval as well as computing a rolling
## average (to plot, for example).  But the concept is here now and we
## can build off of it.
worker_load <- function(con, keys, worker_ids) {
  logs <- worker_log_tail(con, keys, worker_ids, Inf)
  logs <- logs[order(logs$time), ]

  logs$worker <- 0
  logs$worker[logs$command == "ALIVE"] <- 1
  logs$worker[logs$command == "STOP"] <- -1
  logs$worker_cum <- cumsum(logs$worker)

  logs$task <- 0
  logs$task[logs$command == "TASK_START"] <- 1
  logs$task[logs$command == "TASK_COMPLETE"] <- -1
  logs$task_cum <- cumsum(logs$task)

  logs$dt <- c(diff(logs$time), 0)
  logs$ago <- logs$time[nrow(logs)] - logs$time

  class(logs) <- c("worker_load", class(logs))
  logs
}

##' @export
mean.worker_load <- function(x, time = c(1, 5, 15, Inf), ...) {
  ## make this slightly nicer to work with:
  x$dt <- x$dt * 1000
  x$ago <- x$ago * 1000

  f <- function(t) {
    i <- which(x$ago < t)[[1L]] - 1L
    y <- x[i:nrow(x), ]
    if (i > 0) {
      dt <- y$ago[[1L]] - t
      y$dt[[1L]] <- y$dt[[1L]] - dt
      y$time[[1L]] <- y$time[[1L]] - dt
      y$ago[[1L]] <- t
    }
    c(used = sum(y$task_cum * y$dt) / y$ago[[1L]],
      available = sum(y$worker_cum * y$dt) / y$ago[[1L]])
  }
  res <- vapply(time, f, numeric(2))
  colnames(res) <- as.character(time)
  res
}


tasks_wait <- function(con, keys, task_ids, timeout, time_poll = NULL,
                       progress = NULL, key_complete = NULL) {
  if (is.null(key_complete)) {
    done <- rep(FALSE, length(task_ids))
    fetch <- function() {
      done[!done] <<- hash_exists(con, keys$task_result, task_ids[!done], TRUE)
      done
    }
    general_poll(fetch, time_poll %||% 0.05, timeout, "tasks", TRUE, progress)
  } else {
    time_poll <- time_poll %||% 1
    assert_integer_like(time_poll)
    if (time_poll < 1L) {
      stop("time_poll cannot be less than 1 if using key_complete")
    }
    done <- set_names(
      hash_exists(con, keys$task_result, task_ids, TRUE),
      task_ids)

    fetch <- function() {
      tmp <- con$BLPOP(key_complete, time_poll)
      if (!is.null(tmp)) {
        done[[tmp[[2L]]]] <<- TRUE
      }
      done
    }
    general_poll(fetch, 0, timeout, "tasks", TRUE, progress)
  }

  task_results(con, keys, task_ids)
}


rrq_db <- function(con, keys) {
  redux::storr_redis_api(keys$db_prefix, con)
}


queue_remove <- function(con, keys, task_ids, queue) {
  if (length(task_ids) == 0) {
    return(invisible(logical(0)))
  }
  if (length(queue) > 1) {
    tmp <- split(task_ids, queue)
    res <- Map(function(i, q) i[queue_remove(con, keys, i, q)], tmp, names(tmp))
    return(invisible(task_ids %in% unlist(res)))
  }
  key_queue <- rrq_key_queue(keys$queue_id, queue)
  res <- con$pipeline(
    redux::redis$LRANGE(key_queue, 0, -1),
    redux::redis$DEL(key_queue))
  ids <- list_to_character(res[[1L]])
  keep <- !(ids %in% task_ids)
  if (any(keep)) {
    con$RPUSH(key_queue, ids[keep])
  }
  invisible(task_ids %in% ids)
}

verify_dependencies_exist <- function(controller, depends_on) {
  if (!is.null(depends_on)) {
    dependencies_exist <- controller$task_exists(depends_on)
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
