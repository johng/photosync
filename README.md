# photocache

Browse your entire NAS photo library as if it were local. No waiting, no manual syncing, no duplicates.

photocache is a FUSE filesystem for macOS that mounts your NAS photos as a regular folder. Photos you actually look at get cached locally for instant access. Everything else streams transparently from the NAS. Write a photo and it saves to your local disk immediately, then syncs to the NAS in the background within seconds.

**Why?** NAS photo libraries are slow to browse. Cloud sync tools copy everything. photocache gives you the speed of local storage with the capacity of your NAS, using only the disk space you choose.

## Features

- **On-demand caching** -- open a photo and its entire directory gets cached locally
- **LRU eviction** -- least-recently-used directories are automatically evicted when the cache is full
- **Write-local-first** -- new and edited photos save instantly to cache, then flush to NAS in the background
- **Finder integration** -- green dot for cached files, orange dot for pending NAS sync
- **NAS change detection** -- files moved or deleted on the NAS are detected and cache updated within 60 seconds
- **Crash-safe** -- partial caches from interrupted sessions are cleaned up on next mount
- **Clean browsing** -- Synology metadata, macOS resource forks, and `.DS_Store` files are hidden

## Quick start

```bash
# Prerequisites: macFUSE
brew install macfuse
# Allow kernel extension in System Settings > Privacy & Security, then reboot

# Install
make install

# Mount your NAS and start browsing
sudo mount -t nfs -o vers=3,nolock,resvport 192.168.50.21:/volume1/media ~/nas_media
photocache mount
```

Open `~/NAS Pictures` in Finder. Directories cache automatically as you browse.

## Make targets

| Target | Description |
|--------|-------------|
| `make install` | Build, install to `/usr/local/bin`, init config |
| `make upgrade` | Rebuild and replace binary, restart service |
| `make uninstall` | Stop service, remove binary |
| `make service-start` | Start as a background launchd service |
| `make service-stop` | Stop the background service |
| `make service-restart` | Restart the service |
| `make test` | Run tests |
| `make clean` | Remove build artifacts |

## Commands

| Command | Description |
|---------|-------------|
| `photocache mount` | Mount the FUSE filesystem |
| `photocache unmount` | Unmount the filesystem |
| `photocache status` | Show cache usage, cached directories, pending writes |
| `photocache init` | Create config file and cache directories |
| `photocache clear` | Wipe the local cache (unmount first) |

## Configuration

Config is created at `~/.photo_cache/config.json` on first `init`:

```json
{
  "nas_photos_path": "/Users/johng/nas_media/photos",
  "cache_dir": "/Users/johng/.photo_cache/data",
  "db_path": "/Users/johng/.photo_cache/cache.db",
  "mount_point": "/Users/johng/NAS Pictures",
  "max_cache_bytes": 53687091200
}
```

`max_cache_bytes` controls the local cache size limit (default 50 GB).

## Background service

Run photocache automatically at login:

```bash
make service-start    # install and start
make service-stop     # stop
make service-restart  # restart after config changes
```

## Logging

```bash
# Recommended: cache operations
RUST_LOG=photocache::sync=info photocache mount

# Verbose: per-file detail
RUST_LOG=photocache::sync=debug photocache mount
```

Service logs: `~/Library/Logs/photocache/`

## How it works

```
~/NAS Pictures (FUSE mount)

  Reads:    cache hit  --> local disk (fast)
            cache miss --> NAS (transparent)

  Writes:   local cache --> background flush to NAS (5s)

  Browsing: merged NAS + cache directory listings

  Tags:     green = cached    orange = pending sync

Background:
  Cache worker   -- caches directories on demand, evicts LRU
  Flush worker   -- syncs writes to NAS, validates NAS state
  Startup        -- cleans partial caches, removes stale entries
```

## Supported formats

jpg, jpeg, png, heic, heif, dng, raw, tiff, tif, cr2, nef, arw, aae, xmp, mov

## Requirements

- macOS with [macFUSE](https://macfuse.github.io/)
- NAS accessible via NFS mount
- Rust toolchain (for building)
