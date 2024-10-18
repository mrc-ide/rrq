##' Configure rrq options. This function must be called before either
##' a controller or worker connects to a queue, as the options will
##' apply to both. The function may only be called once on a given
##' queue as there is no facility (yet) to update options. Currently
##' the options concern only storage, and specifically how larger
##' objects will be saved (using [`rrq::object_store`].
##'
##' @section Storage:
##'
##' Every time that a task is saved, or a task is completed, results
##'   are saved into the Redis database. Because Redis is an in-memory
##'   database, it's not a great idea to save very large objects into
##'   it (if you ran 100 jobs in parallel and each saved a 2GB object
##'   you'd likely take down your redis server). In addition, `redux`
##'   does not support directly saving objects larger than `2^31 - 1`
##'   bytes into Redis. So, for some use cases we need to consider
##'   where to store larger objects.
##'
##' The strategy here is to "offload" the larger objects - bigger than
##'   some user-given size - onto some other storage system. Currently
##'   the only alternative supported is a disk store
##'   ([`rrq::object_store_offload_disk`]) but we hope to expand this
##'   later.  So if your task returns a 3GB object then we will spill
##'   that to disk rather than failing to save that into
##'   Redis.
##'
##' The storage directory for offloading must be shared amoung all
##'   users of the queue. Depending on the usecase, this could be
##'   a directory on the local filesystems or, if using a queue across
##'   machines, it can be a network file system mounted on all machines.
##'   Since the offload directory mount point may be different for each
##'   client, it is a property of the controller instance rather than of
##'   the queue. See the [`rrq_controller`] function for how to configure
##'   the offload.
##'
##' How big is an object? We serialise the object
##'   (`redux::object_to_bin` just wraps [`serialize`]) which creates
##'   a vector of bytes and that is saved into the database. To get an
##'   idea of how large things are you can do:
##'   `length(redux::object_to_bin(your_object))`.  At the time this
##'   documentation was written, `mtcars` was `3807` bytes, and a
##'   million random numbers was `8,000,031` bytes. It's unlikely that
##'   a `store_max_size` of less than 1MB will be sensible.
##'
##' @title Configure rrq
##'
##' @param queue_id The queue id; the same as you would pass to
##'   [rrq::rrq_controller]
##'
##' @param con A redis connection
##'
##' @param ... Additional arguments - this must be empty. This
##'   argument exists so that all additional arguments must be passed
##'   by name.
##'
##' @param store_max_size The maximum object size, in bytes, before
##'   being moved to the offload store. If given, then larger data
##'   will be saved in `offload_path` (using
##'   [`rrq::object_store_offload_disk`])
##'
##' @param offload_path The path to create an offload store at. This
##'   configuration option is deprecated, and the offload path should be
##'   configured at the controller level.
##'
##' @return Invisibly, a list with processed configuration information
##' @export
##' @examplesIf rrq:::enable_examples()
##' tmp <- tempfile()
##' dir.create(tmp)
##' rrq_configure("rrq:offload", store_max_size = 100000)
##' obj <- rrq_controller("rrq:offload", offload_path = tmp)
##' x <- runif(100000)
##' t <- rrq_task_create_expr(mean(x), controller = obj)
##' dir(tmp)
##' file.size(dir(tmp, full.names = TRUE))
##' rrq_destroy(controller = obj)
rrq_configure <- function(queue_id, con = redux::hiredis(), ...,
                          store_max_size = Inf, offload_path = NULL) {
  if (length(list(...)) > 0) {
    ## Could use rlang::check_dot_used here, but that errors at exit,
    ## which is slightly different.
    cli::cli_abort("Unconsumed dot arguments")
  }
  keys <- rrq_keys(queue_id)

  assert_scalar_numeric(store_max_size)
  if (!is.null(offload_path)) {
    assert_scalar_character(offload_path)
    cli::cli_warn(c(
      paste("The {.code offload_path} argument is deprecated. You should pass",
            "it as an argument to {.code rrq_controller} instead."),
      i = paste("If you are seeing this message while using hipercow, you",
                "should update your version of hipercow.")))
  }

  config <- list(store_max_size = store_max_size,
                 offload_path = offload_path)

  prev <- con$GET(keys$configuration)
  if (is.null(prev)) {
    con$SET(keys$configuration, object_to_bin(config))
  } else if (!identical(unserialize(prev), config)) {
    cli::cli_abort(
      "Can't set configuration for queue '{queue_id}' as it already exists")
  }

  invisible(config)
}


rrq_configure_read <- function(con, keys) {
  config <- con$GET(keys$configuration)
  if (is.null(config)) {
    config <- rrq_configure(keys$queue_id, con)
  } else {
    config <- bin_to_object(config)
  }
  config
}
