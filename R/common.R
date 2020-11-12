## nolint start
## First, the ideal lifecycle:
## * after submissing a job is pending
TASK_PENDING  <- "PENDING"
## * after it is picked up by a worker it is running
TASK_RUNNING  <- "RUNNING"
## * after it is finished by a worker it is complete or error
TASK_COMPLETE <- "COMPLETE"
TASK_ERROR    <- "ERROR"

## Alternatively:
## worker node died
TASK_ORPHAN   <- "ORPHAN"
## task was interrupted
TASK_INTERRUPTED <- "INTERRUPTED"
## orphaned/interrupted task was redirected
TASK_REDIRECT <- "REDIRECT"
## An unknown task
TASK_MISSING  <- "MISSING"

WORKER_IDLE <- "IDLE"
WORKER_BUSY <- "BUSY"
WORKER_EXITED <- "EXITED"
WORKER_LOST <- "LOST"
WORKER_PAUSED <- "PAUSED"
## nolint end

version_info <- function(package = "rrq") {
  descr <- packageDescription(package)
  version <- package_version(descr$Version)
  repository <- descr$Repository
  sha <- descr$RemoteSha
  list(package = package,
       version = version,
       repository = repository,
       packaged = descr$Packaged %||% "(unknown packaged)",
       sha = sha)
}

version_string <- function(data = version_info()) {
  if (!is.null(data$repository)) {
    qual <- sprintf("%s; %s", data$repository, data$packaged)
  } else if (!is.null(data$sha)) {
    qual <- data$sha
  } else {
    qual <- "LOCAL"
  }
  sprintf("%s [%s]", data$version, qual)
}
