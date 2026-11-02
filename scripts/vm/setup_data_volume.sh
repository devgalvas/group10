#!/usr/bin/env bash
# Format (only if empty), mount and lay out the persistent data volume at /data.
# Idempotent: safe to rerun. Usage: sudo scripts/vm/setup_data_volume.sh /dev/vdb [owner]
set -euo pipefail

DEVICE="${1:?usage: $0 <device, e.g. /dev/vdb> [owner]}"
OWNER="${2:-ubuntu}"
MOUNT_POINT=/data

if [[ $EUID -ne 0 ]]; then
  echo "run as root (sudo)" >&2
  exit 1
fi
if [[ ! -b "$DEVICE" ]]; then
  echo "$DEVICE is not a block device" >&2
  exit 1
fi
if findmnt -rn -S "$DEVICE" | grep -qv "^$MOUNT_POINT "; then
  echo "$DEVICE is already mounted somewhere other than $MOUNT_POINT; refusing" >&2
  exit 1
fi

# Never mkfs a device that already holds a filesystem or partition table.
FSTYPE="$(blkid -o value -s TYPE "$DEVICE" || true)"
if [[ -z "$FSTYPE" ]]; then
  if [[ -n "$(lsblk -n -o NAME "$DEVICE" | tail -n +2)" ]]; then
    echo "$DEVICE has partitions; refusing to format" >&2
    exit 1
  fi
  echo "formatting $DEVICE as ext4"
  mkfs.ext4 -q -L ais-data "$DEVICE"
elif [[ "$FSTYPE" != "ext4" ]]; then
  echo "$DEVICE holds a $FSTYPE filesystem; refusing to touch it" >&2
  exit 1
else
  echo "$DEVICE already has ext4; not formatting"
fi

UUID="$(blkid -o value -s UUID "$DEVICE")"
mkdir -p "$MOUNT_POINT"
if ! grep -q "UUID=$UUID" /etc/fstab; then
  echo "UUID=$UUID $MOUNT_POINT ext4 defaults,nofail 0 2" >> /etc/fstab
  echo "added $MOUNT_POINT to /etc/fstab"
fi
systemctl daemon-reload
mountpoint -q "$MOUNT_POINT" || mount "$MOUNT_POINT"

mkdir -p "$MOUNT_POINT"/ais/{dev,prod}/{landing,lake} "$MOUNT_POINT"/spark-events
chown -R "$OWNER:$OWNER" "$MOUNT_POINT"

df -h "$MOUNT_POINT"
