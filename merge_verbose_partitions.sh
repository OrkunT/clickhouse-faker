#!/usr/bin/env bash
# merge_partitions.sh – compact drill_events one partition at a time
# usage:  bash merge_partitions.sh

set -euo pipefail
TABLE="drill_events"
MEM_CAP="20G"
GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'   # colour codes

echo -e "${GREEN}Merging partitions for table $TABLE …${NC}"

# fetch partitions into an array
readarray -t PARTS < <(
  clickhouse-client -q "
     SELECT DISTINCT partition
     FROM system.parts
     WHERE table = '$TABLE' AND active
     ORDER BY partition"
)

TOTAL=${#PARTS[@]}
printf "Found %d active partitions\n\n" "$TOTAL"

COUNTER=0
for P in "${PARTS[@]}"; do
  COUNTER=$((COUNTER+1))
  printf "[%d/%d] %-10s " "$COUNTER" "$TOTAL" "$P"
  START=$(date +%s)

  # how many active parts before the merge?
  BEFORE=$(clickhouse-client -q "
      SELECT count() FROM system.parts
      WHERE table='$TABLE' AND active AND partition='$P'")

  if clickhouse-client -q "
        OPTIMIZE TABLE $TABLE
        PARTITION '$P'
        FINAL
        SETTINGS max_memory_usage='$MEM_CAP'"; then

      AFTER=$(clickhouse-client -q "
          SELECT count() FROM system.parts
          WHERE table='$TABLE' AND active AND partition='$P'")

      END=$(date +%s)
      ELAPSED=$((END-START))
      printf "${GREEN}✓ merged${NC}  "
      printf "(parts: %s → %s, %s s)\n" "$BEFORE" "$AFTER" "$ELAPSED"
  else
      printf "${RED}✗ failed${NC}\n"
  fi
done

echo -e "\n${GREEN}All partitions processed.${NC}"

