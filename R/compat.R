##' @export
##' @keywords internal
##' @noRd
rrq_controller2 <- function(...) {
  deprecated("rrq_controller2", "rrq_controller")
  rrq_controller(...)
}


##' @export
##' @keywords internal
##' @noRd
rrq_worker_config_save2 <- function(...) {
  deprecated("rrq_worker_config_save2", "rrq_worker_config_save")
  rrq_worker_config_save(...)
}


##' @export
##' @keywords internal
##' @noRd
rrq_worker_spawn2 <- function(...) {
  deprecated("rrq_worker_spawn2", "rrq_worker_spawn")
  rrq_worker_spawn(...)
}


deprecated <- function(old, new) {
  cli::cli_warn(
    "'{old}()' is deprecated, please use '{new}()' instead",
    .frequency = "regularly",
    .frequency_id = sprintf("deprecated_%s", old))
}
