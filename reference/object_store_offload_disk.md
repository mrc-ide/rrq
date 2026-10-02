# Disk-based offload

Disk-based offload

Disk-based offload

## Details

A disk-based offload for
[`object_store`](https://mrc-ide.github.io/rrq/reference/object_store.md).
This is not intended at all for direct user-use.

## Methods

### Public methods

- [`object_store_offload_disk$new()`](#method-object_store_offload_disk-new)

- [`object_store_offload_disk$mset()`](#method-object_store_offload_disk-mset)

- [`object_store_offload_disk$mget()`](#method-object_store_offload_disk-mget)

- [`object_store_offload_disk$mdel()`](#method-object_store_offload_disk-mdel)

- [`object_store_offload_disk$list()`](#method-object_store_offload_disk-list)

- [`object_store_offload_disk$destroy()`](#method-object_store_offload_disk-destroy)

------------------------------------------------------------------------

### Method `new()`

Create the store

#### Usage

    object_store_offload_disk$new(path)

#### Arguments

- `path`:

  A directory name to store objects in. It will be created if it does
  not yet exist.

------------------------------------------------------------------------

### Method `mset()`

Save a number of values to disk

#### Usage

    object_store_offload_disk$mset(hash, value)

#### Arguments

- `hash`:

  A character vector of object hashes

- `value`:

  A list of serialised objects (each of which is a raw vector)

------------------------------------------------------------------------

### Method [`mget()`](https://rdrr.io/r/base/get.html)

Retrieve a number of objects from the store

#### Usage

    object_store_offload_disk$mget(hash)

#### Arguments

- `hash`:

  A character vector of hashes of the objects to return. The objects
  will be deserialised before return.

------------------------------------------------------------------------

### Method `mdel()`

Delete a number of objects from the store

#### Usage

    object_store_offload_disk$mdel(hash)

#### Arguments

- `hash`:

  A character vector of hashes to remove

------------------------------------------------------------------------

### Method [`list()`](https://rdrr.io/r/base/list.html)

List hashes stored in this offload store

#### Usage

    object_store_offload_disk$list()

------------------------------------------------------------------------

### Method `destroy()`

Completely delete the store (by deleting the directory)

#### Usage

    object_store_offload_disk$destroy()
