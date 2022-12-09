# Map Reduce Report

- [Map Reduce Report](#map-reduce-report)
  - [Introduction](#introduction)
  - [Single Node](#single-node)
    - [Reader Threads](#reader-threads)
    - [Mapper Threads](#mapper-threads)
    - [Reducer Threads](#reducer-threads)
    - [Performance](#performance)
  - [Multi-Node](#multi-node)
    - [Performance](#performance-1)
  - [Results](#results)
    - [Bottlenecks](#bottlenecks)
    - [Load Imbalances](#load-imbalances)

## Introduction

We chose to use C++, OpenMP, and MPI as our core tools for implementing a solution. C++ reduces the complexity of creating custom types for maps, hash functions, queues, and vectors that already exist in a higher level language, yet retains performance gains from a garbage collection free, compiled language.

As laid out from the project description, we had the following  core items:

 - Reader threads which read files and put data onto a work queue.
 - Mapper threads which create combined records of words.
 - Reducer threads that read from a queue and combine counts for each word.

## Single Node

### Reader Threads

Each reader thread has access to:

 - A list of files.
 - A thread safe line queue.

The list of files is thread safe for each queue. A lock is used for each line queue, to prevent race conditions between the reader threads and mapper threads for lines.

### Mapper Threads

Each mapper thread has access to:

 - A custom thread safe line queue.
 - A map used as a thread local data structure to store words as keys and counts as values.
 - A list of thread safe reducer queues.
 - A hash function for hashing words to a reducer queue.

The mapper thread reads lines from the line queue and inserts words and counts to a thread local map. If the word exists, the count is simply incremented.

The mapper thread also creates a combined record of word counts by hashing the word to value between 0 and the number of reducer queues. This value is used as an index for selecting the queue in which the word and count pair should be placed.

### Reducer Threads

Each reducer thread has access to:

 - A list of thread safe reducer queues.
 - A resulting reducer map

The reducer threads read items from their thread's queue and combine them to a single record in the map. If the word doesn't exist in the map, the pair is inserted. If the word does exist, the count is simply updated.

### Performance

The speedup formula is as follows:

$$ S = { T_1 \over T_p } $$

where $T_1$ is the execution time for one core and $T_p$ is execution time for $p$ cores.

The efficiency formula is as follows:

$$ E = { T_1 \over p * T_p } $$

where $T_1$ is the execution time for one core, p is the number of cores, and $T_p$ is execution time for $p$ cores.

The Karp-Flatt Metric is as follows:

$$ e = { { {1 \over S} - {1 \over p} } \over { 1 - { 1 \over p } } } $$

where $S$ is speedup and $p$ is number of cores.

The resulting table below is from processing the provided files.

| Thread/Core count | 1   | 2    | 4    | 8    | 16   |
| ----------------- | --- | ---- | ---- | ---- | ---- |
| Speedup           | 1   | 1.78 | 2.78 | 3.11 | 3.60 |
| Efficiency        | 1   | 0.89 | 0.70 | 0.39 | 0.22 |
| Karp-Flatt Metric | N/A | 0.12 | 0.15 | 0.22 | 0.23 |

TODO: Graphs showing number of readers, mappers, and reducers and performance

## Multi-Node

Each node has the same single node implementation, with an added complexity.

The number of reader, mapper, and reducer threads is multiplied by the number of nodes in the network. So, hashed words may be sent to *any* other node and core in the network, further distributing the work load to all cores on all nodes.

### Performance

TODO: Speedup, efficiency, and Karp-Flatt analysis on 2,4,8,16 nodes

| Thread count      | 1   | 2   | 4   | 8   | 16  |
| ----------------- | --- | --- | --- | --- | --- |
| Speedup           |     |     |     |     |     |
| Efficiency        |     |     |     |     |     |
| Karp-Flatt Metric |     |     |     |     |     |

## Results

TODO: Synthesize results and explain speedups

### Bottlenecks

TODO: Note bottle neck of mappers to reducers

### Load Imbalances

TODO: Insert graphs of time spent in each thread and note ways to increase balancing. Potential case for a better hash function implementation?