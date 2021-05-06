rrq_scripts_load <- function(con) {
  lapply(rrq_scripts(), con$SCRIPT_LOAD)
}


rrq_scripts <- function() {
  ## This needs to be done in a script to be an atomic deletion
  ## operation.
  queue_delete <- "
local key_task_queue = KEYS[1]
local task_id = ARGV[1]
local key_queue = redis.call('HGET', key_task_queue, task_id)
return redis.call('LREM', key_queue, 0, task_id)"
  list(queue_delete = queue_delete)
}
