## TODO: The prefix will need some work to avoid collisions with other
## people.  Avoid the system RNG too - might need to go with openssl
## for real RNGs, or with uuids.
rrq_keys <- function(queue_name, worker_name = NULL) {
  if (is.null(worker_name)) {
    rrq_keys_queue(queue_name)
  } else {
    c(rrq_keys_queue(queue_name),
      rrq_keys_worker(queue_name, worker_name))
  }
}

rrq_keys_queue <- function(queue) {
  list(queue_name     = queue,

       controllers    = sprintf("rrq:%s:controllers",     queue),

       workers_name   = sprintf("rrq:%s:workers:name",    queue),
       workers_status = sprintf("rrq:%s:workers:status",  queue),
       workers_task   = sprintf("rrq:%s:workers:task",    queue),
       workers_info   = sprintf("rrq:%s:workers:info",    queue),

       queue_rrq      = sprintf("rrq:%s:queue:rrq:id",    queue),
       queue_ctx      = sprintf("rrq:%s:queue:ctx:id",    queue),

       tasks_expr     = sprintf("rrq:%s:tasks:expr",      queue),
       tasks_status   = sprintf("rrq:%s:tasks:status",    queue),
       tasks_worker   = sprintf("rrq:%s:tasks:worker",    queue),
       tasks_result   = sprintf("rrq:%s:tasks:result",    queue),
       tasks_complete = sprintf("rrq:%s:tasks:complete",  queue))
}

rrq_keys_worker <- function(queue, worker) {
  list(message  = rrq_key_worker_message(queue, worker),
       response = rrq_key_worker_response(queue, worker),
       log      = rrq_key_worker_log(queue, worker))
}

## Special key for worker-specific commands to be published to.
rrq_key_worker_message <- function(queue, worker) {
  sprintf("rrq:%s:workers:%s:message", queue, worker)
}
rrq_key_worker_response <- function(queue, worker) {
  sprintf("rrq:%s:workers:%s:response", queue, worker)
}
rrq_key_worker_log <- function(queue, worker) {
  sprintf("rrq:%s:workers:%s:log", queue, worker)
}

## Randomly generated keys:
rrq_key_task_complete <- function(queue) {
  sprintf("rrq:%s:tasks:complete:%s", queue, ids::random_id())
}

##' Randomly generate key for polling new workers
##' @title Randomly generate key for polling new workers
##' @param queue_name Name of the queue (the context id)
##' @export
##' @author Rich FitzJohn
rrq_key_worker_alive <- function(queue_name) {
  sprintf("rrq:%s:workers:alive:%s", queue_name, ids::random_id())
}
