# photocache

A FUSE filesystem that keeps a local cache of your latest photos from a NAS, serving them instantly while transparently fetching older ones over the network.

## How it works

- Mounts a virtual directory at `~/Photos` that shows **all** your NAS photos
- The newest 50GB (configurable) are cached locally for instant access
- Older photos are read directly from the NAS on demand
- New photos you add are written through to the NAS immediately and cached locally
- A background sync daemon keeps the cache up to date every 30 minutes

## Prerequisites

macFUSE is required:

```bash
brew install macfuse
# Allow the kernel extension in System Settings > Privacy & Security
# Reboot
```

## Install

```bash
cd photo_cache
cargo build --release
```

The binary is at `target/release/photocache`.

## Setup

```bash
# 1. Mount your NAS (if not already)
sudo mount -v -t nfs -o vers=3,nolock,resvport 192.168.50.21:/volume1/media /Users/johng/nas_media

# 2. Initialize config and cache directories
./target/release/photocache init

# 3. Run the first sync (copies newest 50GB to local cache)
./target/release/photocache sync

# 4. Mount the virtual filesystem
./target/release/photocache mount
```

Your photos are now available at `~/Photos`.

## Commands

| Command | Description |
|---------|-------------|
| `photocache init` | Create config file and cache directories |
| `photocache sync` | Run a sync cycle (cache newest photos, evict oldest) |
| `photocache status` | Show cache usage, file count, and paths |
| `photocache mount` | Mount the FUSE filesystem at `~/Photos` |
| `photocache unmount` | Unmount the filesystem |
| `photocache clear` | Wipe the local cache |

## Configuration

Config lives at `~/.photo_cache/config.json`:

```json
{
  "nas_photos_path": "/Users/johng/nas_media/photos",
  "cache_dir": "/Users/johng/.photo_cache/data",
  "db_path": "/Users/johng/.photo_cache/cache.db",
  "mount_point": "/Users/johng/Photos",
  "max_cache_bytes": 53687091200,
  "sync_interval_minutes": 30
}
```

- `max_cache_bytes` — Local cache size limit (default: 50GB)
- `sync_interval_minutes` — How often the background sync runs (default: 30)

## Run as a background service

Install the launchd services to auto-mount at login and sync every 30 minutes:

```bash
cp launchd/com.johng.photocache-mount.plist ~/Library/LaunchAgents/
cp launchd/com.johng.photocache-sync.plist ~/Library/LaunchAgents/

launchctl load ~/Library/LaunchAgents/com.johng.photocache-mount.plist
launchctl load ~/Library/LaunchAgents/com.johng.photocache-sync.plist
```

To stop the services:

```bash
launchctl unload ~/Library/LaunchAgents/com.johng.photocache-mount.plist
launchctl unload ~/Library/LaunchAgents/com.johng.photocache-sync.plist
```

## Logs

- Mount: `~/.photo_cache/mount.log` / `~/.photo_cache/mount.err.log`
- Sync: `~/.photo_cache/sync.log` / `~/.photo_cache/sync.err.log`

Enable verbose logging with `RUST_LOG=debug photocache mount`.

## Architecture

```
~/Photos (FUSE mount)
    |
    +-- Read request --> Check ~/.photo_cache/data/ (local cache)
    |                       |
    |                       +--> HIT: serve from local disk
    |                       +--> MISS: read from ~/nas_media/photos/ (NAS)
    |
    +-- Write request --> Write to NAS --> Copy to local cache
    |
    +-- Directory listing --> Merge NAS + cache entries

Background sync (every 30 min):
    1. Scan NAS for all photos
    2. Sort by modification time (newest first)
    3. Cache newest files up to 50GB limit
    4. Evict oldest cached files when over limit
```

## Supported formats

jpg, jpeg, png, heic, heif, dng, raw, tiff, tif, cr2, nef, arw
