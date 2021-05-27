redis_dump <- function(key, con) {
  type <- as.character(con$TYPE(key))
  data <- switch(type,
                 list = con$LRANGE(key, 0, -1),
                 string = con$GET(key),
                 hash = con$HGETALL(key),
                 stop(sprintf("Can't dump type '%s'", type)))
  list(key = key, type = type, data = data)
}


redis_restore <- function(value, con) {
  switch(value$type,
         list = con$RPUSH(value$key, value$data),
         string = con$SET(value$key, value$data),
         hash = con$command(list("HMSET", value$key, value$data)),
         stop(sprintf("Can't dump type '%s'", type)))
}
