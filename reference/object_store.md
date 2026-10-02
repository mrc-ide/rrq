# rrq object store

When you create a task with rrq and that task uses local variables these
need to be copied over to the worker that will evaluate the task. So, if
we had

    rrq_task_create_expr(f(a, b))

that would be the objects `a` and `b` from the context where
`rrq_task_create_expr` was called. There are a few considerations here:

- The names `a` and `b` are only useful in the immediate context of the
  controller at the point the task is sent and so we need to store the
  *values* referenced by `a` and `b` without reference to the names - we
  do this by naming the new values after their value. That is, the name
  becomes the hash of the object, computed by
  [`rlang::hash()`](https://rlang.r-lib.org/reference/hash.html), as a
  form of [content-addressable
  storage](https://en.wikipedia.org/wiki/Content-addressable_storage).

- When doing this we note that we might end up using the value
  referenced by `a` or `b` many times in different tasks so we should
  not re-save the data more than needed, and we should not necessarily
  delete it when a task is deleted unless nothing else uses that value.

- The objects might tiny or could be large; if small we tend to care
  about how quickly they can be resolved (i.e., latency) and if large we
  need to be careful not to overfull Redis' database as it's a
  memory-based system.

To make this robust and flexible, we use a `object_store` object, which
will allow objects to be stored either directly in Redis, or offloaded
onto some "large" data store based on their size. Currently, we provide
support only for offloading to disk, but in future hope to expand this.

When we create a value in the store (or reference a value that already
exists) we assign a tag into the database; this means that we have for a
value with hash `abc123` and tag `def789`

- `prefix:data["abc123"] => [1] f5 26 a5 b7 26 93 b3 41 b7 d0 b0...`
  (the data stored, serialised into a redis hash by its hash, as a
  binary object.

- `prefix:tag_hash:def789 => {abc123}` (a set of hashes used by our tag)

- `prefix:hash_tag:abc123 => {def789}` (a set of tags that reference our
  hash)

If we also used the value with hash `abc123` with tag `fed987` this
would look like

- `prefix:data[abc123] => [1] f5 26 a5 b7 26 93 b3 41 b7 d0 b0...` hash,
  as a binary object.

- `prefix:tag_hash:def789 => {abc123}`

- `prefix:tag_hash:fed987 => {abc123}`

- `prefix:hash_tag:abc123 => {def789, fed987}`

As tags are dropped, then the references are dropped from the set
`prefix:hash_tag:abc123` and when that set becomes empty then we can
delete `prefix:data[abc123]` as simple form of [reference
counting](https://en.wikipedia.org/wiki/Reference_counting).

For `rrq` we will use `task_id`s as a tag.

For dealing with large data, we "offload" large data into a secondary
store. This replaces the redis hash of `hash => value` with something
else. Currently the only alternative we offer is
[`object_store_offload_disk`](https://mrc-ide.github.io/rrq/reference/object_store_offload_disk.md)
which will save the binary representation of the object at the path
`<path>/<hash>` and will allow large values to be shared between
controller and worker so long as they share a common filesystem.

## Details

Create an object store. Typically this is not used by end-users, and is
used internally by
[rrq_controller](https://mrc-ide.github.io/rrq/reference/rrq_controller.md)

## Methods

### Public methods

- [`object_store$new()`](#method-object_store-new)

- [`object_store$list()`](#method-object_store-list)

- [`object_store$tags()`](#method-object_store-tags)

- [`object_store$get()`](#method-object_store-get)

- [`object_store$mget()`](#method-object_store-mget)

- [`object_store$set()`](#method-object_store-set)

- [`object_store$mset()`](#method-object_store-mset)

- [`object_store$location()`](#method-object_store-location)

- [`object_store$drop()`](#method-object_store-drop)

- [`object_store$destroy()`](#method-object_store-destroy)

------------------------------------------------------------------------

### Method `new()`

Create a new object store (or connect to an existing one)

#### Usage

    object_store$new(con, prefix, max_size = Inf, offload = NULL)

#### Arguments

- `con`:

  A redis connection object

- `prefix`:

  A key prefix to use; we will make a number of keys that start with
  this prefix.

- `max_size`:

  The maximum serialised object size, in bytes. If the serialised object
  is larger than this size it will be placed into the offload storage,
  as provided by the `offload` argument. By default this is `Inf` so all
  values will be stored in the redis database.

- `offload`:

  An offload storage object. We provide one of these
  [`object_store_offload_disk`](https://mrc-ide.github.io/rrq/reference/object_store_offload_disk.md),
  which saves objects to on disk after serialisation). This interface is
  subject to change. If not given but an object exceeds `max_size` an
  error will be thrown.

------------------------------------------------------------------------

### Method [`list()`](https://rdrr.io/r/base/list.html)

List all hashes of data known to this data store

#### Usage

    object_store$list()

------------------------------------------------------------------------

### Method `tags()`

List all tags known to this data store

#### Usage

    object_store$tags()

------------------------------------------------------------------------

### Method [`get()`](https://rdrr.io/r/base/get.html)

Get a single object by its hash

#### Usage

    object_store$get(hash)

#### Arguments

- `hash`:

  a single hash to use

------------------------------------------------------------------------

### Method [`mget()`](https://rdrr.io/r/base/get.html)

Get a number objects by their hashes. Unlike `$get()` this method
accepts a vector of hash (length 0, 1, or more than 1) and returns a
list of the same length.

#### Usage

    object_store$mget(hash)

#### Arguments

- `hash`:

  A vector of object hashes

------------------------------------------------------------------------

### Method `set()`

Set an object into the object store, returning the hash of that object.

#### Usage

    object_store$set(value, tag, serialize = TRUE)

#### Arguments

- `value`:

  The object to save

- `tag`:

  A string used to associate with the object. When all tags that point
  to a particular object value have been removed, then the object will
  be deleted from the store.

- `serialize`:

  Logical, indicating if the values should be serialised first.
  Typically this should be `TRUE`, but for advanced use if you already
  have a serialised object you can pass that in and set to `FALSE`. Note
  that only objects serialised with
  [`redux::object_to_bin`](https://richfitz.github.io/redux/reference/object_to_string.html)
  (or with `serialize(..., xdr = FALSE)`) will be accepted.

------------------------------------------------------------------------

### Method `mset()`

Set a number of objects into the store. Unlike `$set()`, this method
sets a list of objects into the store at once, and returns a character
vector of hashes the same length as the list of values.

#### Usage

    object_store$mset(value, tag, serialize = TRUE)

#### Arguments

- `value`:

  A list of objects to save

- `tag`:

  A string used to associate with the object. When all tags that point
  to a particular object value have been removed, then the object will
  be deleted from the store. The same tag is used for all objects.

- `serialize`:

  Logical, indicating if the values should be serialised first.
  Typically this should be `TRUE`, but for advanced use if you already
  have a serialised object you can pass that in and set to `FALSE`. Note
  that only objects serialised with
  [`redux::object_to_bin`](https://richfitz.github.io/redux/reference/object_to_string.html)
  (or with `serialize(..., xdr = FALSE)`) will be accepted.

------------------------------------------------------------------------

### Method `location()`

Return the storage locations of a set of hashes. Currently the location
may be `redis` (stored directly in the redis server), `offload` (stored
in the offload storage) or `NA` (if not found, and if `error = FALSE`).

#### Usage

    object_store$location(hash, error = TRUE)

#### Arguments

- `hash`:

  A vector of hashes

- `error`:

  A logical, indicating if we should throw an error if a hash is unknown

------------------------------------------------------------------------

### Method [`drop()`](https://rdrr.io/r/base/drop.html)

Delete tags from the store. This will dissociate the tags from any
hashes they references and if that means that no tag points to a hash
then the data at that hash will be removed. We return (invisibly) a
character vector of any dropped hashes.

#### Usage

    object_store$drop(tag)

#### Arguments

- `tag`:

  Vector of tags to drop

------------------------------------------------------------------------

### Method `destroy()`

Remove all data from the store, and all the stores metadata

#### Usage

    object_store$destroy()
