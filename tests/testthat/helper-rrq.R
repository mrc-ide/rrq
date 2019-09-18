test_queue_clean <- function(context_id, delete=TRUE) {
  invisible(rrq_clean(redux::hiredis(), context_id, delete, "message"))
}

temp_context <- function(sources=NULL, ...) {
  root <- tempfile()
  dir.create(root, TRUE, FALSE)
  if (length(sources) > 0L) {
    file.copy(sources, root)
  }
  context::context_save(root, sources=sources, ...)
}

worker_command <- function(obj) {
  root <- obj$context$root$path
  context_id <- obj$context$id
  bquote(rrq_worker_from_config(.(root), .(context_id), "localhost"))
}

has_internet <- function() {
  !is.null(suppressWarnings(utils::nsl("www.google.com")))
}

skip_if_no_internet <- function() {
  if (has_internet()) {
    return()
  }
  testthat::skip("no internet")
}

skip_if_no_redis <- function() {
  tryCatch(
    redux::hiredis()$PING(),
    error = function(e) testthat::skip("redis not available"))
  invisible(NULL)
}

wait_status <- function(t, obj, timeout = 2, time_poll = 0.05,
                        status = "PENDING") {
  times_up <- queuer:::time_checker(timeout)
  while (!times_up()) {
    if (all(obj$task_status(t) != status)) {
      return()
    }
    message(".")
    Sys.sleep(time_poll)
  }
  stop(sprintf("Did not change status to %s in time", status))
}


test_context <- function(sources = NULL) {
  root <- tempfile()
  dir.create(root)
  if (length(sources) > 0) {
    file.copy(sources, root)
  }

  context <- with_wd(root, {
    ## TODO: I think that this should be
    ##
    ##   file.path(root, "context")
    ##
    ## but that causes a worker load failure
    ctx <- context::context_save(root, sources = sources)
    context::context_load(ctx, new.env(parent = .GlobalEnv))
  })

  context
}


test_rrq <- function(sources = NULL) {
  skip_if_no_redis()
  Sys.setenv(R_TESTS = "")
  context <- test_context(sources)
  obj <- rrq_controller(context, redux::hiredis())
  reg.finalizer(obj, function(e) obj$destroy())
  obj
}


## TODO: I wonder if the path should be set automatically?
test_worker_spawn <- function(obj, ..., timeout = 10) {
  worker_spawn(obj, ..., path = obj$context$root$path, progress = PROGRESS,
               timeout = timeout)
}


with_wd <- function(path, expr) {
  if (path != ".") {
    if (!file.exists(path)) {
      stop(sprintf("Path '%s' does not exist", path))
    }
    if (!is_directory(path)) {
      stop(sprintf("Path '%s' exists, but is not a directory", path))
    }
    owd <- setwd(path)
    on.exit(setwd(owd))
  }
  force(expr)
}


PROGRESS <- FALSE # TODO: phase this one out
options(queuer.progress_suppress = TRUE)
