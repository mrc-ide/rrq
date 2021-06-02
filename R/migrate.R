##' Migrate the rrq internal format after breaking changes. We take a
##' "best-effort" approach to migrate data, but generally one should
##' not assume that the rrq data store is a persistent store. If you
##' try to connect to a database that has not been migrated, an error
##' will be thrown indicating how you can run this function.
##'
##' @section Compatibility:
##'
##' We can detect if a rrq database has been opened with a newer
##'   version of rrq and attempt to fix it. However, older versions do
##'   not now know when the database has become out of date. Using the
##'   same version of worker and controller will generally give good
##'   results though.
##'
##' @section 0.3.1 to 0.4.0:
##'
##' We moved from an internal data store based on `storr` to one that
##'   allows offloading. This requires moving all data around to the
##'   new store and rewriting tasks to reference it (both task inputs
##'   and task outputs).
##'
##' @title Migrate rrq's internal database and storage
##'
##' @param queue_id The name of the rrq queue (as passed to
##'   [rrq::rrq_controller]
##'
##' @param con A redis connection
##'
##' @return Nothing, called for its side effects
##' @export
rrq_migrate <- function(queue_id, con = redux::hiredis()) {
  keys <- rrq_keys(queue_id)
  db <- rrq_migrate_check(con, keys, FALSE)
  if (is.null(db)) {
    message("rrq database is up-to-date")
    return(invisible())
  }

  message("Migrating rrq database")
  store <- object_store$new(con, keys$object_store)

  changed <- vlapply(con$HKEYS(keys$task_expr), rrq_migrate_task_expr,
                     con, keys, db, store)
  message(sprintf("Updated %d / %d task expressions",
                  sum(changed), length(changed)))

  changed <- vlapply(con$HKEYS(keys$task_result), rrq_migrate_task_result,
                     con, keys, store)
  message(sprintf("Updated %d task results", length(changed)))

  db$destroy()
}


rrq_migrate_check <- function(con, keys, error) {
  db_prefix <- sprintf("%s:db:", keys$queue_id)
  if (con$EXISTS(paste0(db_prefix, ":config:hash_algorithm")) == 0) {
    return(NULL)
  }
  if (error) {
    stop(sprintf(
      'rrq database needs migrating; please run\n  rrq::rrq_migrate("%s")',
      keys$queue_id))
  }
  redux::storr_redis_api(db_prefix, con)
}


rrq_migrate_task_expr <- function(id, con, keys, db, store) {
  d <- redux::bin_to_object(con$HGET(keys$task_expr, id))
  changed <- FALSE

  if (!is.null(d$function_hash) && nchar(d$function_hash) == 32L) {
    function_value <- db$get(d$function_hash)
    d$function_hash <- store$set(function_value, id)
    d$expr[[1]] <- as.name(d$function_hash)
    changed <- TRUE
  }

  if (length(d$objects) > 0 && nchar(d$objects[[1]]) == 32L) {
    data <- db$mget(d$objects, use_cache = FALSE)
    d$objects[] <- store$mset(data, id)
    changed <- TRUE
  }

  if (changed) {
    con$HSET(keys$task_expr, id, object_to_bin(d))
  }

  changed
}


rrq_migrate_task_result <- function(id, con, keys, store) {
  changed <- FALSE
  result <- con$HGET(keys$task_result, id)
  if (is.raw(result)) {
    hash <- store$set(result, id, FALSE)
    con$HSET(keys$task_result, id, hash)
    changed <- TRUE
  }
  changed
}
