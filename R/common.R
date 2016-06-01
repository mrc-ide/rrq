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
## orphaned task was requeued
TASK_REDIRECT <- "REDIRECT"
## An unknown task
TASK_MISSING  <- "MISSING"


version_info <- function(package=.packageName) {
  descr <- packageDescription(package)
  version <- package_version(descr$Version)
  repository <- descr$Repository
  sha <- descr$RemoteSha
  list(package=package,
       version=version,
       repository=repository,
       sha=sha)
}

version_string <- function() {
  data <- version_info()
  if (!is.null(data$repository)) {
    qual <- data$repository
  } else if (!is.null(data$sha)) {
    qual <- data$sha
  } else {
    qual <- "LOCAL"
  }
  sprintf("%s [%s]", data$version, qual)
}
