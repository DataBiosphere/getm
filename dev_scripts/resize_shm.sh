#!/usr/bin/bash
# Resize available shared memory on a linux system:
#   dev_scripts/resize_shm.sh 8G
#   dev_scripts/resize_shm.sh 64M
# This should be executed with 'sudo', or as root.
new_size=$1
mount -o remount,size=${new_size} /dev/shm
