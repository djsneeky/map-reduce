#include "MapReduce.h"

#include <map>
#include <queue>
#include <string>
#include <fstream>
#include <sstream>
#include <cstdbool>
#include <iostream>
#include <omp.h>
#include <filesystem>

// openmp function that runs on each proc
void mapReduceParallel()
{
    int max_threads = omp_get_max_threads();

    // queues for lines - one for each mapper thread
    std::vector<std::queue<std::string>> lineQueues;
    for (int i = 0; i < max_threads; i++)
    {
        lineQueues.push_back(std::queue<std::string>());
    }

    // word maps - one for each mapper thread
    std::vector<std::map<std::string, int>> wordMaps;
    for (int i = 0; i < max_threads; i++)
    {
        wordMaps.push_back(std::map<std::string, int>());
    }

    // hash function for <string, int> pair reducer id
    const std::hash<std::string> wordHashFn;

    // create reducer queues
    std::vector<std::queue<std::pair<std::string, int>>> reducerQueues;
    for (int i = 0; i < max_threads; i++)
    {
        reducerQueues.push_back(std::queue<std::pair<std::string, int>>());
    }

    // create reducer maps
    std::vector<std::map<std::string, int>> reducerMaps;
    for (int i = 0; i < max_threads; i++)
    {
        reducerMaps.push_back(std::map<std::string, int>());
    }

    #pragma omp parallel
    {
        // Reader threads
        #pragma omp single nowait
        {
            // reader thread reads files and put lines of file into queues
            // reader thread requests file when no remaining work
            populateLineQueues("../test/files/jungle.txt", lineQueues);
        }

        // Mapper threads
        #pragma omp single nowait
        {
            // get the thread id
            int thread_id = omp_get_thread_num();
            // check to see that there are lines available in the queue
            while (!lineQueues[thread_id].empty())
            {
                // map thread to read line queues and place words into thread local word map
                populateWordMap(lineQueues[thread_id].front(), wordMaps[thread_id], ' ');
                lineQueues[thread_id].pop();
            }
            // map map thread will put pairs onto queues for for each reducer
            // split words in map to other 'reducer' queues
            unsigned int reducerQueueId;
            std::map<std::string, int>::iterator it;
            for (it = wordMaps[thread_id].begin(); it != wordMaps[thread_id].end(); it++)
            {
                // get the dest reducer id
                reducerQueueId = getReducerQueueId(it->first, wordHashFn, max_threads);
                // send to reducer 
                reducerQueues[reducerQueueId].push(std::make_pair(it->first, it->second));
                // remove pair from map
                wordMaps[thread_id].erase(it);
            }
        }

        // Reducer threads
        #pragma omp single nowait
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
 * @brief Reads a file line by line and loops through a vector, populating line queues
 * 
 * @param fileName the path of the file
 * @param lineQueues the vector of queues
 * @return true if the file has been processed successfully, false otherwise
 */
bool populateLineQueues(const std::string &fileName, std::vector<std::queue<std::string>> &lineQueues)
{
    std::ifstream file(fileName);
    int i = 0;
    if (file.is_open())
    {
        std::string line;
        while (std::getline(file, line))
        {
            lineQueues[i++].push(line);
            i %= lineQueues.size();
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
 * @brief Reads a file line by line and populates a line queue
 *
 * Used by the reader threads
 *
 * @param fileName the absolute path of the file
 * @param lineQueue the queue to populate
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

void addTestFiles(const std::string &dirPath, std::queue<std::string> &testFiles)
{
    std::queue<std::string> testFilePaths;
    std::string testPath = "../test/files";
    for (const auto & entry : std::filesystem::directory_iterator(testPath))
    {
        testFiles.push(entry.path().string());
    }
}
    