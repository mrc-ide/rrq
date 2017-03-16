heartbeat <- function(con, key, period) {
  if (!is.null(period)) {
    context::context_log("heartbeat", key)
    loadNamespace("heartbeatr")
    config <- con$config()
    ret <- heartbeatr::heartbeat(key, period,
                                 host = config$host, port = config$port,
                                 password = config$password, db = config$db)
    context::context_log("heartbeat", "OK")
    ret
  }
}
