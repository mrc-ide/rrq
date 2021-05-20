##' @title rrq object store
##'
##' Create an object store. Typically this is not used by end-users,
##' and is used internally by [rrq::rrq_controller]
##'
##' When you create a task with rrq and that task uses local variables
##' these need to be copied over to the worker that will evaluate the
##' task. So, if we had
##'
##' ```
##' obj$enqueue(f(a, b))
##' ```
##'
##' that would be the objects `a` and `b` from the context where
##' `enqueue` was called. There are a few considerations here:
##'
##' * The names `a` and `b` are only useful in the immediate context
##'   of the controller at the point the task is sent and so we need
##'   to store the *values* referenced by `a` and `b` without
##'   reference to the names - we do this by naming the new values
##'   after their value. That is, the name becomes the hash of the
##'   object, computed by [openssl::sha256()] after serialisation, as
##'   a form of [content-addressible
##'   storage](https://en.wikipedia.org/wiki/Content-addressable_storage).
##' * When doing this we note that we might end up using the value
##'   referenced by `a` or `b` many times in different tasks so we
##'   should not re-save the data more than needed, and we should not
##'   necessarily delete it when a task is deleted unless nothing else
##'   uses that value.
##' * The objects might tiny or could be large; if small we tend to
##'   care about how quickly they can be resolved (i.e., latency) and
##'   if large we need to be careful not to overfull Redis' database
##'   as it's a memory-based system.
##'
##' To make this robust and flexible, we use a `object_store` object,
##' which will allow objects to be stored either directly in Redis, or
##' offloaded onto some "large" data store based on their
##' size. Currently, we provide support only for offloading to disk,
##' but in future hope to expand this.
##'
##' When we create a value in the store (or reference a value that
##' already exists) we assign a tag into the database; this means that
##' we have for a value with hash `abc123` and tag `def789`
##'
##' * `prefix:data["abc123"] => [1] f5 26 a5 b7 26 93 b3 41 b7 d0 b0...`
##'   (the data stored, serialised into a redis hash by its hash, as a
##'   binary object.
##' * `prefix:tag_hash:def789 => {abc123}` (a set of hashes used by our tag)
##' * `prefix:hash_tag:abc123 => {def789}` (a set of tags that
##'   reference our hash)
##'
##' If we also used the value with hash `abc123` with tag `fed987`
##' this would look like
##'
##' * `prefix:data[abc123] => [1] f5 26 a5 b7 26 93 b3 41 b7 d0 b0...`
##'   hash, as a binary object.
##' * `prefix:tag_hash:def789 => {abc123}`
##' * `prefix:tag_hash:fed987 => {abc123}`
##' * `prefix:hash_tag:abc123 => {def789, fed987}`
##'
##' As tags are dropped, then the references are dropped from the set
##' `prefix:hash_tag:abc123` and when that set becomes empty then we
##' can delete `prefix:data[abc123]` as simple form of [reference
##' counting](https://en.wikipedia.org/wiki/Reference_counting).
##'
##' For `rrq` we will use `task_id`s as a tag.
##'
##' For dealing with large data, we "offload" large data into a
##' secondary store. This replaces the redis hash of `hash => value`
##' with something else. Currently the only alternative we offer is
##' [`rrq::object_store_offload_disk`] which will save the binary
##' representation of the object at the path `<path>/<hash>` and will
##' allow large values to be shared between controller and worker so
##' long as they share a common filesystem.
##'
##' @export
object_store <- R6::R6Class(
  "object_store",
  cloneable = FALSE,

  public = list(
    ##' @description Create a new object store (or connect to an existing one)
    ##'
    ##' @param con A redis connection object
    ##'
    ##' @param prefix A key prefix to use; we will make a number of keys
    ##'   that start with this prefix.
    ##'
    ##' @param max_size The maximum serialised object size, in bytes.
    ##'   If the serialised object is larger than this size it will
    ##'   be placed into the offload storage, as provided by the
    ##'   `offload` argument. By default this is `Inf` so all values will
    ##'   be stored in the redis database.
    ##'
    ##' @param offload An offload storage object. We provide one of
    ##'   these [`rrq::object_store_offload_disk`], which saves objects
    ##'   to on disk after serialisation). This interface is
    ##'   subject to change. If not given but an object execeeds `max_size`
    ##'   an error will be thrown.
    initialize = function(con, prefix, max_size = Inf, offload = NULL) {
      private$con <- con
      private$keys <- list(
        data = paste0(prefix, ":data"),
        location = paste0(prefix, ":location"),
        tag_hash = paste0(prefix, ":tag_hash:%s"),
        hash_tag = paste0(prefix, ":hash_tag:%s"))
      private$max_size <- max_size
      private$offload <- offload %||% object_store_offload_null$new()
    },

    ##' @description List all hashes of data known to this data store
    list = function() {
      c(list_to_character(private$con$HKEYS(private$keys$data)),
        private$offload$list())
    },

    ##' @description Get a single object by its hash
    ##'
    ##' @param hash a single hash to use
    get = function(hash) {
      assert_scalar_character(hash)
      self$mget(hash)[[1L]]
    },

    ##' @description Get a number objects by their hashes. Unlike `$get()` this
    ##' method accepts a vector of hash (length 0, 1, or more than 1)
    ##' and returns a list of the same length.
    ##'
    ##' @param hash A vector of object hashes
    mget = function(hash) {
      if (length(hash) == 0) {
        return(list())
      }
      keys <- private$keys
      i <- self$location(hash, TRUE) == "redis"
      data <- vector("list", length(hash))
      if (any(i)) {
        data[i] <- lapply(private$con$HMGET(keys$data, hash[i]), bin_to_object)
      }
      if (any(j <- !i)) {
        data[j] <- private$offload$mget(hash[j])
      }
      data
    },

    ##' @description Set an object into the object store, returning the hash
    ##' of that object.
    ##'
    ##' @param value The object to save
    ##'
    ##' @param tag A string used to associate with the object. When
    ##' all tags that point to a particular object value have been
    ##' removed, then the object will be deleted from the store.
    set = function(value, tag) {
      self$mset(list(value), tag)[[1L]]
    },

    ##' @description Set a number of objects into the store. Unlike `$set()`,
    ##' this method sets a list of objects into the store at once,
    ##' and returns a character vector of hashes the same length as the
    ##' list of values.
    ##'
    ##' @param value A list of objects to save
    ##'
    ##' @param tag A string used to associate with the object. When
    ##' all tags that point to a particular object value have been
    ##' removed, then the object will be deleted from the store.
    ##' The same tag is used for all objects.
    mset = function(value, tag) {
      assert_is(value, "list")
      assert_scalar_character(tag)
      if (length(value) == 0) {
        return(character(0))
      }

      keys <- private$keys
      data <- lapply(value, object_to_bin)
      hash <- vcapply(data, hash_data)

      cmd_set <- NULL
      cmd_loc <- NULL

      ## Store any bits of data are are not yet known
      i <- is.na(self$location(hash, FALSE))
      if (any(i)) {
        j <- lengths(data[i]) <= private$max_size
        if (any(j)) {
          cmd_set <- redis$HMSET(keys$data, hash[i][j], data[i][j])
        }
        loc <- rep("redis", sum(i))
        if (any(k <- !j)) {
          private$offload$mset(hash[i][k], data[i][k])
          loc[k] <- "offload"
        }
        cmd_loc <- redis$HMSET(keys$location, hash[i], loc)
      }

      ## And associate data with a tag
      cmd_tag <- redis$SADD(sprintf(keys$tag_hash, tag), hash)
      cmd_hash <- lapply(sprintf(keys$hash_tag, hash), redis$SADD, tag)

      cmds <- c(list(cmd_set, cmd_loc, cmd_tag), cmd_hash)
      private$con$pipeline(.commands = cmds)

      hash
    },

    ##' @description Return the storage locations of a set of hashes. Currently
    ##' the location may be `redis` (stored directly in the redis server),
    ##' `offload` (stored in the offload storage) or `NA` (if not found,
    ##' and if `error = FALSE`).
    ##'
    ##' @param hash A vector of hashes
    ##'
    ##' @param error A logical, indicating if we should throw an error if
    ##' a hash is unknown
    location = function(hash, error = TRUE) {
      loc <- private$con$HMGET(private$keys$location, hash)
      msg <- vlapply(loc, is.null)
      if (any(msg)) {
        if (error) {
          stop("Some hashes were not found!")
        }
        loc[msg] <- NA_character_
      }
      list_to_character(loc)
    },

    ##' @description Delete tags from the store. This will dissociate the
    ##' tags from any hashes they references and if that means that no tag
    ##' points to a hash then the data at that hash will be removed. We return
    ##' (invisibly) a character vector of any dropped hashes.
    ##'
    ##' @param tag Vector of tags to drop
    drop = function(tag) {
      if (length(tag) == 0) {
        return()
      }

      keys <- private$keys
      key_tag_hash <- sprintf(keys$tag_hash, tag)

      hash <- private$con$pipeline(
        .commands = c(lapply(key_tag_hash, redis$SMEMBERS),
                      lapply(key_tag_hash, redis$DEL)))[seq_along(tag)]
      hash <- unique(unlist(hash))

      if (length(hash) == 0) {
        return()
      }

      key_hash <- sprintf(keys$hash_tag, list_to_character(hash))

      cmd <- c(lapply(key_hash, redis$SREM, tag),
               lapply(key_hash, redis$EXISTS))
      res <- private$con$pipeline(.commands = cmd)
      drop <- vnapply(res, identity)[-seq_along(hash)] == 0
      if (any(drop)) {
        i <- self$location(hash[drop]) == "redis"
        if (any(i)) {
          private$con$HDEL(keys$data, hash[drop][i])
        }
        if (any(j <- !i)) {
          private$offload$mdel(hash[drop][j])
        }
        private$con$HDEL(keys$location, hash[drop])
      }
      invisible(hash[drop])
    },

    ##' @description Remove all data from the store, and all the stores
    ##' metadata
    destroy = function() {
      private$offload$destroy()
      private$con$DEL(private$keys$data)
      private$con$DEL(private$keys$location)
      redux::scan_del(private$con, sprintf(private$keys$hash_tag, "*"))
      redux::scan_del(private$con, sprintf(private$keys$tag_hash, "*"))
      invisible()
    }
  ),

  private = list(
    con = NULL,
    keys = NULL,
    max_size = NULL,
    offload = NULL
  ))


## The offload structure is really simple; we need a way of writing
## and reading. I am unsure if we're best off doing
## multiple-by-default or single-by-default here, but have written
## this to assumme multiple as that will help with.
##
## mset: raw => storage
## mget: storage => raw => object
##
## i.e., the offload has responsiblity for deserialising too. This
## avoids the cost of loading the binary data into memory, then
## deserialising it, when it can be done from a stream.
##
## This is written so that later we can extend it, but for now we will
## always use this class really (I would like to see how we get on
## with something like sql storage or S3 buckets, etc, later,
## especially in conjunction with a threshhold of about 10MB so that
## we're restricted to fairly slow bits of data anyway; practically
## this may require some sort of local cache and too).

##' A disk-based offload for [`rrq::object_store`]. This is not
##' intended at all for direct user-use.
##'
##' @title Disk-based offload
object_store_offload_disk <- R6::R6Class(
  "object_store_offload_disk",
  cloneable = FALSE,

  public = list(
    ##' @description Create the store
    ##'
    ##' @param path A directory name to store objects in. It will be
    ##'   created if it does not yet exist.
    initialize = function(path) {
      dir.create(path, FALSE, TRUE)
      private$path <- normalizePath(path, mustWork = TRUE)
    },

    ##' @description Save a number of values to disk
    ##'
    ##' @param hash A character vector of object hashes
    ##'
    ##' @param value A list of serialised objects
    ##'   (each of which is a raw vector)
    mset = function(hash, value) {
      dest <- private$filename(hash)
      for (i in seq_along(hash)) {
        write_bin(value[[i]], dest[[i]])
      }
    },

    ##' @description Retrieve a number of objects from the store
    ##'
    ##' @param hash A character vector of hashes of the objects to return.
    ##'   The objects will be deserialised before return.
    mget = function(hash) {
      lapply(private$filename(hash), readRDS)
    },

    ##' @description Delete a number of objects from the store
    ##'
    ##' @param hash A character vector of hashes to remove
    mdel = function(hash) {
      unlink(private$filename(hash))
    },

    ##' @description List hashes stored in this offload store
    list = function() {
      dir(private$path)
    },

    ##' @description Completely delete the store (by deleting the directory)
    destroy = function() {
      unlink(private$path, recursive = TRUE)
    }
  ),

  private = list(
    path = NULL,
    filename = function(hash) {
      file.path(private$path, hash)
    }
  ))


object_store_offload_null <- R6::R6Class(
  "object_store_offload_null",
  cloneable = FALSE,

  public = list(
    mset = function(hash, value) {
      private$fail()
    },

    mget = function(hash) {
      private$fail()
    },

    mdel = function(hash) {
      private$fail()
    },

    list = function() {
      character(0)
    },

    destroy = function() {
    }
  ),

  private = list(
    fail = function() {
      stop("offload is not supported")
    }
  ))
