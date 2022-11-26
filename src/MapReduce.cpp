#include "MapReduce.h"

#include <map>
#include <queue>
#include <string>
#include <fstream>
#include <sstream>
#include <cstdbool>
#include <iostream>

#include <omp.h>

// openmp function that runs on each proc
void mapReduceParallel()
{
    // initialize queues for lines - one for each mapper thread
    std::vector<std::queue<std::string>> lineQueues;

    #pragma omp parallel
    {
        #pragma omp single
        {
            // reader thread reads files and put lines of file into queues
            // reader thread requests file when no remaining work
        }

        #pragma omp single
        {
            // map thread to read line queues and place words into thread local word map
            // map map thread will put pairs onto queues for for each reducer
        }

        #pragma omp single
        {
            // reducer thread receives pair from queue and updates its map
        }
    }
}

/**
 * @brief A serial implementation of the map reduce algorithm
 *
 * @return true if run successfully, false otherwise
 */
bool mapReduceSerial()
{
    // used in mod operation to determine final reducer queue
    unsigned int maxReducers = 8;
    const std::hash<std::string> wordHashFn;
    size_t hash;

    // read file and put lines into a queue
    std::queue<std::string> lineQueue;
    std::map<std::string, int> wordMap;
    if (!populateLineQueue("../test/files/jungle.txt", lineQueue))
    {
        return false;
    }

    // read lines out of queue and populate a map
    while (!lineQueue.empty())
    {
        populateWordMap(lineQueue.front(), wordMap, ' ');
        lineQueue.pop();
    }

    // split words in map to other 'reducer' queues
    unsigned int reducerQueueId;
    std::map<std::string, int>::iterator it;
    for (it = wordMap.begin(); it != wordMap.end(); it++)
    {
        reducerQueueId = getReducerQueueId(it->first, wordHashFn, maxReducers);
    }

    return true;
}

/**
 * @brief Takes a word and a hash function and returns its desired reducer queue destination
 *
 * @param wordMap a reference to the word map to reduce
 * @param wordHashFn a reference to the hash function
 * @param maxReducers the max number of reducers used
 *
 * @return the reducer queue id number
 */
unsigned int getReducerQueueId(const std::string &word, const std::hash<std::string> &wordHashFn, int maxReducers)
{
    unsigned int num = wordHashFn(word) % maxReducers;

    return num;
}

/**
 * @brief Reads a file line by line and populates a line queue
 *
 * Used by the reader threads
 *
 * @param fileName the absolute path of the file
 * @return true if the file has been processed successfully, false otherwise
 */
bool populateLineQueue(const std::string &fileName, std::queue<std::string> &lineQueue)
{
    std::ifstream file(fileName);
    if (file.is_open())
    {
        std::string line;
        while (std::getline(file, line))
        {
            lineQueue.push(line);
        }
        file.close();
        return true;
    }
    else
    {
        return false;
    }
}

/**
 * @brief Takes a line and puts words into a map
 *
 * Used by the mapper threads
 *
 * @param line a string to process
 * @param word_map a reference to the map to populate
 * @param delim the delimiter used to find word boundaries
 */
void populateWordMap(const std::string &line, std::map<std::string, int> &wordMap, char delim)
{
    std::istringstream iss(line);
    std::string word;
    while (std::getline(iss, word, delim))
    {
        wordMap[word]++;
    }
}