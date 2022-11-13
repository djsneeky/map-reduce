#include <iostream>
#include <map>
#include <queue>
#include <string>
#include <fstream>
#include <sstream>
#include <vector>

#include <omp.h>
#include <mpi.h>

// OpenMP threads are readers, mappers, and reducers

int main() {
    std::cout << "Map Reduce: " << num_procs << " procs, " << num_threads << " threads per proc\n";

    // master process keeps list of files

    // master process still performs map reduce
    mapReduce();

    // master process is notified when all processors have finished

    return 0;
}

// openmp function that runs on each proc
void mapReduce()
{
    std::map<std::string, int> word_map;
    std::queue<std::string> line_q;     // one for each map thread

    // reader thread requests file when no remaining work

    // reader thread reads files and put lines of file into queues

    // map thread will 'map' words into a local word_map
    // for words in line, word_map[word]++

    // map words to a queue for reducers

    // reducer receives pair from queue and updates reducer_map
}

// function to read file and add lines to queue
void populateLineQueue(std)
{
    
}

/**
 * @brief 
 * 
 * @param line 
 * @param word_map 
 */
void populateWordMap(const std::string& line, std::map<std::string, int>& word_map, char delim)
{
    std::istringstream iss(line);
    std::string word;
    while (std::getline(iss, word, delim)) {
        word_map[word]++;
    }
}