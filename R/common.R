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
## task was cancelled
TASK_CANCELLED <- "CANCELLED"
## task (or its worker) was killed or crashed
TASK_DIED <- "DIED"
## task took too long and was stopped due to timeout
TASK_TIMEOUT <- "TIMEOUT"
## An unknown task
TASK_MISSING  <- "MISSING"

## Dependency tasks lifecycle:
## waiting for a dependency before being added to queue
TASK_DEFERRED <- "DEFERRED"
## dependency errored so this tasks condition can never be satisfied
TASK_IMPOSSIBLE <- "IMPOSSIBLE"

WORKER_IDLE <- "IDLE"
WORKER_BUSY <- "BUSY"
WORKER_EXITED <- "EXITED"
WORKER_LOST <- "LOST"
WORKER_PAUSED <- "PAUSED"

QUEUE_DEFAULT <- "default"
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
