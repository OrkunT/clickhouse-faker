#!/bin/bash
echo "Merging each active partition …"
for p in $(clickhouse-client -q "
           SELECT DISTINCT partition FROM system.parts
           WHERE table='drill_events' AND active
           ORDER BY partition"); do
    echo "Partition $p"
    clickhouse-client -q "
        OPTIMIZE TABLE drill_events
        PARTITION '$p'
        FINAL
        SETTINGS max_memory_usage='20G'"
done
echo "✓ All partitions merged."

