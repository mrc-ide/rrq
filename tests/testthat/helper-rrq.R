test_queue_clean <- function(context_id, delete=TRUE) {
  invisible(rrq_clean(redux::hiredis(), context_id, delete, "message"))
}

## Looks like a bug to me, relative to the docs:
PSKILL_SUCCESS <- tools::pskill(Sys.getpid(), 0)
pid_exists <- function(pid) {
  tools::pskill(pid, 0) == PSKILL_SUCCESS
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
  bquote(rrq_worker(context::context_handle(.(root), .(id)),
                    redux::hiredis()),
         obj$context)
}
