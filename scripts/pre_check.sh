#!/bin/bash

# Verify disk and partition status on a physical machine
# 1. Check if disks are formatted without partitions
# 2. Check partition table type (GPT/MBR)
# 3. Check each partition's filesystem type
# Requires root privileges

# Check for root access
if [[ $EUID -ne 0 ]]; then
    echo "Please run this script with root privileges!"
    exit 1
fi

# Get all physical disks (excluding partitions)
DISKS=$(lsblk -dno NAME,TYPE | awk '$2 == "disk" {print "/dev/" $1}')

echo "Starting disk and partition check..."

# Iterate through each disk
for DISK in $DISKS; do
    echo "Checking disk: $DISK"

    # Check if the disk itself has a filesystem (unpartitioned but formatted)
    FS_DISK=$(blkid -o value -s TYPE "$DISK" 2>/dev/null)
    if [[ -n $FS_DISK ]]; then
        echo "  Disk is formatted without partitions, filesystem type: $FS_DISK"
        continue  # Skip further checks for partitions
    fi

    # Detect partition table type
    PARTITION_INFO=$(file -s "$DISK" 2>/dev/null)
    PARTITION_TYPE="No partition table"
    if [[ $PARTITION_INFO =~ "GPT" ]]; then
        PARTITION_TYPE="GPT"
    elif [[ $PARTITION_INFO =~ "DOS/MBR" ]]; then
        PARTITION_TYPE="MBR"
    fi
    echo "  Partition table type: $PARTITION_TYPE"

    # Get all partitions of this disk (supports both SATA/SCSI and NVMe formats)
    # Fix: Handle NVMe partitions like 'nvmeXn1p1'
    PARTITIONS=$(lsblk -lno NAME -p | grep -E "^$DISK(p[0-9]+|[0-9]+)$")

    if [[ -z $PARTITIONS ]]; then
        echo "  This disk has no partitions"
    else
        echo "  Partitions:"
        for PART in $PARTITIONS; do
            FS_TYPE=$(blkid -o value -s TYPE "$PART" 2>/dev/null)
            if [[ -n $FS_TYPE ]]; then
                echo "    $PART: Formatted, filesystem type: $FS_TYPE"
            else
                echo "    $PART: Not formatted"
            fi
        done
    fi
done

echo "Check completed!"
