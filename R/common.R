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


TASK <- list(
  ## Possible status for all known tasks (i.e. all non-missing statuses)
  all = c(TASK_PENDING, TASK_RUNNING, TASK_COMPLETE, TASK_ERROR, TASK_CANCELLED,
          TASK_DIED, TASK_TIMEOUT, TASK_IMPOSSIBLE, TASK_DEFERRED),
  ## Possible status for all finished but incomplete/failed tasks
  terminal_fail = c(TASK_ERROR, TASK_CANCELLED, TASK_DIED, TASK_TIMEOUT,
                    TASK_IMPOSSIBLE),
  ## Possible status for all non-started tasks
  unstarted = c(TASK_PENDING, TASK_DEFERRED),
  ## Possible status for all non-finished tasks
  unfinished = c(TASK_PENDING, TASK_DEFERRED, TASK_RUNNING))

## Possible status for all finished tasks
TASK$terminal <- c(TASK$terminal_fail, TASK_COMPLETE)

WORKER_IDLE <- "IDLE"
WORKER_BUSY <- "BUSY"
WORKER_EXITED <- "EXITED"
WORKER_LOST <- "LOST"
WORKER_PAUSED <- "PAUSED"

QUEUE_DEFAULT <- "default"
## nolint end

## This version number needs to point at the last breaking change to
## the data layout. When either a worker or controller connects to the
## db they'll compare their version against this and report back if
## they disagree.
rrq_schema_version <- "0.4.0"

version_info <- function(package = "rrq") {
  as.character(packageVersion(package))
}
