# Map Reduce Report

## Implementation

We chose to use C++, OpenMP, and MPI as our core tools for implementing a solution. C++ reduces the complexity of creating custom types for maps, hash functions, queues, and vectors that already exist in a higher level language, yet retains performance gains from a garbage collection free, compiled language.

As laid out from the project description, we had the following  core items:

 - Reader threads which read files and put data onto a work queue.
 - Mapper threads which create combined records of words.
 - Reducer threads that read from a queue and combine counts for each word.

At the start of the program, several elements are initialized that are used later on. We will note these as they are used in the program.

### Reader Threads

Each reader thread has access to:

 - A `std::vector<std::string>` of files.
 - A custom `line_queue_t` struct that contains a `std::vector<std::string> line` and a `omp_lock_t lock`.

The list of files is thread safe for each queue. A lock is used for each `std::vector<std::string> line` or list of lines, to prevent race conditions between the reader threads and mapper threads for lines.

### Mapper Threads

Each mapper thread has access to:

 - A custom `line_queue_t` struct that contains a `std::vector<std::string> line` and a `omp_lock_t lock`.
 - A `std::map<std::string, int> wordMap` used as a local map for pairs of words with counts. 
 - A `std::vector<reducer_queue_t*> reducerQueues` where `reducer_queue_t` contains a `std::queue<std::pair<std::string, int>> wordQueue` and a `omp_lock_t lock`
 - A `unsigned int reducerCount` for mapping pairs to a reducer queue
 - A `const std::hash<std::string> wordHashFn` for hashing words to a reducer queue.

The mapper thread reads lines from the `line_queue_t` struct and adds words to a thread specific `wordMap`. If the word exists, the count is simply incremented.

The mapper thread also creates a combined record of `std::pair<std::string, int> pairs` by hashing the word on the `wordHashFn` with a resulting value between 0 and `reducerCount`. This value is then used as an index for selecting a reducer queue to place the `std::pair<std::string, int> pair` in.

The reducer queue must also be protected by a lock, since it is used by the reducer thread. Each mapper thread is mapped 1 to 1 statically to a reducer thread by using OpenMP's `omp_get_thread_num()` in a parallel region.

### Reducer Threads

Each reducer thread has access to:

