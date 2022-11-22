#include "MapReduce.h"

#include <map>
#include <queue>
#include <string>
#include <fstream>
#include <sstream>

// openmp function that runs on each proc
void mapReduce()
{
    std::map<std::string, int> wordMap;
    std::queue<std::string> lineQueue;     // one for each map thread

    // reader thread requests file when no remaining work

    // reader thread reads files and put lines of file into queues

    // map thread will 'map' words into a local word_map
    // for words in line, word_map[word]++

    // map words to a queue for reducers

    // reducer receives pair from queue and updates reducer_map
}

// function to read file and add lines to queue
void populateLineQueue()
{
    
}

/**
 * @brief Takes a line and puts words into a map
 * 
 * @param line a string to process
 * @param word_map a reference to the map to populate
 * @param delim the delimiter used to find word boundaries
 */
void populateWordMap(const std::string& line, std::map<std::string, int>& wordMap, char delim)
{
    std::istringstream iss(line);
    std::string word;
    while (std::getline(iss, word, delim)) {
        wordMap[word]++;
    }
}