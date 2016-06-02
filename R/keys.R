## TODO: The prefix will need some work to avoid collisions with other
## people.  Avoid the system RNG too - might need to go with openssl
## for real RNGs, or with uuids.
rrq_keys <- function(queue_name, worker_name=NULL) {
  if (is.null(worker_name)) {
    rrq_keys_queue(queue_name)
  } else {
    c(rrq_keys_queue(queue_name),
      rrq_keys_worker(queue_name, worker_name))
  }
}

rrq_keys_queue <- function(queue) {
  list(queue_name      = queue,

       workers_name    = sprintf("rrq:%s:workers:name",    queue),
       workers_status  = sprintf("rrq:%s:workers:status",  queue),
       workers_task    = sprintf("rrq:%s:workers:task",    queue),
       workers_info    = sprintf("rrq:%s:workers:info",    queue),

       tasks_queue     = sprintf("rrq:%s:tasks:id",        queue),
       tasks_expr      = sprintf("rrq:%s:tasks:expr",      queue),
       tasks_status    = sprintf("rrq:%s:tasks:status",    queue),
       tasks_worker    = sprintf("rrq:%s:tasks:worker",    queue),
       tasks_result    = sprintf("rrq:%s:tasks:result",    queue),
       tasks_complete  = sprintf("rrq:%s:tasks:complete",  queue))
}

rrq_keys_worker <- function(queue, worker) {
  list(message   = rrq_key_worker_message(queue, worker),
       response  = rrq_key_worker_response(queue, worker),
       log       = rrq_key_worker_log(queue, worker))
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
rrq_key_worker_alive <- function(queue) {
  sprintf("rrq:%s:workers:alive:%s", queue, ids::random_id())
}

parse_worker_log <- function(log) {
  re <- "^([0-9]+) ([^ ]+) ?(.*)$"
  ok <- grepl(re, log)
  if (!all(ok)) {
    stop("Corrupt log")
  }
  time <- as.integer(sub(re, "\\1", log))
  command <- sub(re, "\\2", log)
  message <- lstrip(sub(re, "\\3", log))
  data.frame(time, command, message, stringsAsFactors=FALSE)
}

task_object_prefix <- function(task_id) {
  sprintf(".%s:", task_id)
}
