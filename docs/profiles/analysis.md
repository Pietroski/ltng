# Performance Analysis Report

## Overview
Analysis of why LTNG Client is slower reaching Docker compared to local server, based on profiling data.

## CPU Profile Analysis

### LTNG Engine
- Duration: 30.03s
- Total samples: 0 (indicates very low CPU usage)
- Main operations:
  - concurrent.New: 73.14% of memory operations
  - No significant CPU bottlenecks identified

### BadgerDB Engine
- Duration: 30.07s
- Total samples: 880ms (2.93% CPU utilization)
- Top CPU consumers:
  1. runtime.futex: 38.64%
  2. runtime.write1: 22.73%
  3. syscall.Syscall6: 15.91%
  4. runtime.findRunnable: 2.27%

## Memory Profile Analysis

### LTNG Engine
- Total memory: 7.7MB
- Key allocations:
  1. concurrent.New: 5.67MB (73.14%)
  2. regexp/syntax.compiler: 544.67kB (7.03%)
  3. makeDeletionChannels: 513.12kB (6.62%)

### BadgerDB Engine
- Total memory: 172.55MB
- Key allocations:
  1. badger/v4/skl.newArena: 166.41MB (96.44%)
  2. ristretto/v2.newCmRow: 2.54MB (1.47%)
  3. ristretto/v2/z.Bloom.Size: 2.31MB (1.34%)

## Network Analysis
The profiling data suggests that the Docker network bottleneck is primarily due to:

1. System Call Overhead:
   - High percentage of time spent in syscalls (15.91%)
   - Significant futex operations (38.64%)
   - Network-related write operations (22.73%)

2. Memory Management Impact:
   - BadgerDB's large memory footprint (172.55MB vs 7.7MB)
   - Heavy memory allocation in Docker container
   - Memory pressure affecting network performance

3. Container Networking:
   - Additional network stack overhead in Docker
   - Container bridge networking adding latency
   - Network namespace transitions

## Recommendations

1. Consider implementing connection pooling to reduce syscall overhead
2. Optimize BadgerDB memory usage, particularly in skl.newArena
3. Evaluate Docker network mode options (host vs bridge)
4. Monitor and tune Docker container resource limits

## Conclusion
The slower performance in Docker is primarily attributed to:
1. Container networking overhead
2. High system call frequency
3. Memory pressure from BadgerDB
