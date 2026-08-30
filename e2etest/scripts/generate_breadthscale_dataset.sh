#!/bin/bash
# generate_breadthscale_dataset.sh
#
# Generates a local hardlink dataset for BreadthScale testing.
# Creates N unique inodes, each with LINKS_PER_INODE hardlinks,
# spread across a shallow directory tree (bucket dirs to avoid
# single-directory inode pressure on ext4/xfs).
#
# Usage:
#   ./generate_breadthscale_dataset.sh <base_dir> <num_inodes> [links_per_inode] [file_size_bytes]
#
# Examples:
#   ./generate_breadthscale_dataset.sh /tmp/hltest_10k   10000
#   ./generate_breadthscale_dataset.sh /tmp/hltest_100k  100000
#   ./generate_breadthscale_dataset.sh /tmp/hltest_1m    1000000 2 1024

set -euo pipefail

BASE_DIR="${1:?Usage: $0 <base_dir> <num_inodes> [links_per_inode] [file_size_bytes]}"
NUM_INODES="${2:?Usage: $0 <base_dir> <num_inodes> [links_per_inode] [file_size_bytes]}"
LINKS_PER_INODE="${3:-2}"
FILE_SIZE="${4:-1024}"  # bytes per anchor file

TOTAL_FILES=$((NUM_INODES * LINKS_PER_INODE))
# Bucket into subdirectories of ~1000 inodes each to avoid filesystem slowdowns
BUCKET_SIZE=1000

echo "=== BreadthScale Hardlink Dataset Generator ==="
echo "Base dir:        $BASE_DIR"
echo "Unique inodes:   $NUM_INODES"
echo "Links per inode: $LINKS_PER_INODE"
echo "File size:       $FILE_SIZE bytes"
echo "Total objects:   $TOTAL_FILES"
echo "Bucket size:     $BUCKET_SIZE inodes/dir"
echo ""

# Pre-flight checks
echo "Checking filesystem limits..."
AVAIL_INODES=$(df -i "$( dirname "$BASE_DIR" )" 2>/dev/null | awk 'NR==2 {print $4}')
if [[ -n "$AVAIL_INODES" && "$AVAIL_INODES" != "-" ]]; then
  if (( AVAIL_INODES < NUM_INODES * 2 )); then
    echo "WARNING: Only $AVAIL_INODES inodes available, need ~$((NUM_INODES * 2)). May run out."
  else
    echo "  Available inodes: $AVAIL_INODES (OK)"
  fi
fi

AVAIL_KB=$(df -k "$( dirname "$BASE_DIR" )" 2>/dev/null | awk 'NR==2 {print $4}')
NEED_KB=$(( (NUM_INODES * FILE_SIZE) / 1024 + 1024 ))  # +1MB headroom
if [[ -n "$AVAIL_KB" ]]; then
  if (( AVAIL_KB < NEED_KB )); then
    echo "WARNING: Only ${AVAIL_KB}KB available, need ~${NEED_KB}KB."
  else
    echo "  Available disk:   ${AVAIL_KB}KB (OK, need ~${NEED_KB}KB)"
  fi
fi
echo ""

# Clean and create base
if [[ -d "$BASE_DIR" ]]; then
  echo "Removing existing directory: $BASE_DIR"
  rm -rf "$BASE_DIR"
fi
mkdir -p "$BASE_DIR"

# Pre-create bucket directories
NUM_BUCKETS=$(( (NUM_INODES + BUCKET_SIZE - 1) / BUCKET_SIZE ))
echo "Creating $NUM_BUCKETS bucket directories..."
for ((b=0; b<NUM_BUCKETS; b++)); do
  mkdir -p "${BASE_DIR}/bucket_$(printf '%04d' $b)"
done

# Generate a reusable random block for file content (avoids per-file /dev/urandom overhead)
RAND_BLOCK=$(mktemp)
trap "rm -f '$RAND_BLOCK'" EXIT
dd if=/dev/urandom of="$RAND_BLOCK" bs="$FILE_SIZE" count=1 status=none

echo "Generating $NUM_INODES inodes × $LINKS_PER_INODE links..."
START_TIME=$(date +%s)
LAST_REPORT=0

for ((i=0; i<NUM_INODES; i++)); do
  BUCKET="bucket_$(printf '%04d' $((i / BUCKET_SIZE)))"
  ANCHOR="${BASE_DIR}/${BUCKET}/file_${i}_anchor.txt"

  # Create anchor file with unique content (copy random block + append inode index)
  cp "$RAND_BLOCK" "$ANCHOR"
  # Append index to make each file's content unique (important for content-based dedup testing)
  printf '%d' "$i" >> "$ANCHOR"

  # Create hardlinks to the anchor
  for ((j=1; j<LINKS_PER_INODE; j++)); do
    ln "$ANCHOR" "${BASE_DIR}/${BUCKET}/file_${i}_link_${j}.txt"
  done

  # Progress reporting every 5000 inodes
  if (( i > 0 && i % 5000 == 0 )); then
    NOW=$(date +%s)
    ELAPSED=$((NOW - START_TIME))
    RATE=0
    if (( ELAPSED > 0 )); then
      RATE=$((i / ELAPSED))
    fi
    echo "  $i / $NUM_INODES inodes (${ELAPSED}s elapsed, ~${RATE} inodes/s)"
    LAST_REPORT=$i
  fi
done

END_TIME=$(date +%s)
TOTAL_ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "=== Dataset Created ==="
echo "Time:            ${TOTAL_ELAPSED}s"
ACTUAL_FILES=$(find "$BASE_DIR" -type f | wc -l)
ACTUAL_INODES=$(find "$BASE_DIR" -type f -printf '%i\n' | sort -u | wc -l)
echo "Total files:     $ACTUAL_FILES"
echo "Unique inodes:   $ACTUAL_INODES"
DISK_USAGE=$(du -sh "$BASE_DIR" | cut -f1)
echo "Disk usage:      $DISK_USAGE"

# Sanity check
if (( ACTUAL_INODES != NUM_INODES )); then
  echo "ERROR: Expected $NUM_INODES unique inodes but got $ACTUAL_INODES"
  exit 1
fi
if (( ACTUAL_FILES != TOTAL_FILES )); then
  echo "ERROR: Expected $TOTAL_FILES files but got $ACTUAL_FILES"
  exit 1
fi

echo ""
echo "Sample inode groups (first 5):"
find "$BASE_DIR" -type f -printf '%i %p\n' | sort -n | head -$((5 * LINKS_PER_INODE))
echo ""
echo "Ready for AzCopy testing: azcopy sync '$BASE_DIR' '<dest>' --hardlinks=preserve --recursive"
