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
#include "FileHelper.h"

typedef struct line_queue
{
    std::queue<std::string> line;
    omp_lock_t lock;
} line_queue_t;

typedef struct reducer_queue
{
    std::queue<std::pair<std::string, int>> wordQueue;
    omp_lock_t lock;
} reducer_queue_t;


// openmp function that runs on each proc
void mapReduceParallel()
{

    int max_threads = omp_get_max_threads();

    std::vector<std::string> testFiles = getListOfTestFiles();;

    // queues for lines - one for each mapper thread
    std::vector<line_queue_t*> lineQueues;
    for (int i = 0; i < max_threads; i++)
    {
        line_queue_t* newQueue = new line_queue_t;
        omp_init_lock(&(newQueue->lock));
        lineQueues.push_back(newQueue);
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
    std::vector<reducer_queue_t*> reducerQueues;
    for (int i = 0; i < max_threads; i++)
    {
        reducer_queue_t* newReducerQueue = new reducer_queue_t;
        omp_init_lock(&(newReducerQueue->lock));
        reducerQueues.push_back(newReducerQueue);
    }

    // create reducer maps
    std::vector<std::map<std::string, int>> reducerMaps;
    for (int i = 0; i < max_threads; i++)
    {
        reducerMaps.push_back(std::map<std::string, int>());
    }

    std::queue<std::string> testFileList;
    // addTestFiles("../test/files", testFileList);

    unsigned int readerThreadCount = omp_get_max_threads();
    unsigned int mapperThreadCount = omp_get_max_threads();
    unsigned int reducerThreadCount = omp_get_max_threads();

    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        // Reader threads
        #pragma omp single nowait
        {
            for (int i = 0; i < readerThreadCount; i++)
            {
                #pragma omp task
                {
                    // takes a file list and populates a thread specific line queue
                    readerTask(testFileList, lineQueues[thread_id]);
                }
            }
        }

        // Mapper threads
        #pragma omp single nowait
        {
            for (int i = 0; i < mapperThreadCount; i++)
            {
                #pragma omp task
                {
                    mapperTask(lineQueues[thread_id], wordMaps[thread_id], reducerQueues);
                }
            }
        }

        // Reducer threads
        #pragma omp single nowait
        {
            // reducer thread receives pair from queue and updates its map
            for (int i = 0; i < reducerThreadCount; i++)
            {
                #pragma omp task
                {
                    reducerTask(reducerQueues[thread_id], reducerMaps[thread_id]);
                }
            }
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
 * @brief 
 * 
 * @param testFileList 
 * @param lineQueue 
 */
void readerTask(std::queue<std::string> &testFileList, line_queue_t* lineQueue)
{
    unsigned int filesRemaining;
    std::string testFile;
    #pragma omp critical
    filesRemaining = testFileList.size();
    while (filesRemaining != 0)
    {
        #pragma omp critical
        {
            testFile = testFileList.front();
            testFileList.pop();
            filesRemaining = testFileList.size();
        }
        populateLineQueue(testFile, lineQueue);
    }
}

void mapperTask(line_queue_t* lineQueue, 
                std::map<std::string, int> &wordMap, 
                std::vector<reducer_queue_t*> &reducerQueues,
                unsigned int reducerCount)
{
    // check to see that there are lines available in the queue
    omp_set_lock(&(lineQueue->lock));
    unsigned int linesRemaining = lineQueue.size();
    omp_unset_lock(&(lineQueue->lock));
    while (linesRemaining != 0)
    {
        // map thread to read line queues and place words into thread local word map
        omp_set_lock(&(lineQueue->lock));
        std::string line = lineQueue.front();
        lineQueue.pop();
        omp_unset_lock(&(lineQueue->lock));
        populateWordMap(line, wordMap, ' ');
    }
    // map thread will put pairs onto queues for for each reducer
    // split words in map to other 'reducer' queues
    unsigned int reducerQueueId;
    std::map<std::string, int>::iterator it;
    for (it = wordMap.begin(); it != wordMap.end(); it++)
    {
        // get the dest reducer id
        reducerQueueId = getReducerQueueId(it->first, wordHashFn, reducerCount);
        // send to reducer
        // TODO: consider vector of pairs to reduce critical section bottleneck
        omp_set_lock(&(reducerQueues[reducerQueueId]->lock));
        reducerQueues[reducerQueueId]->wordQueue.push(std::make_pair(it->first, it->second));
        omp_unset_lock(&(reducerQueues[reducerQueueId]->lock));
        // remove pair from local map
        wordMap.erase(it);
    }
}

void reducerTask(reducer_queue_t* reducerQueue, std::map<std::string, int> reducerMap)
{
    return;
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
bool populateLineQueue(const std::string &fileName, line_queue_t* lineQueue)
{
    std::ifstream file(fileName);
    if (file.is_open())
    {
        std::string newLine;
        while (std::getline(file, newLine))
        {
            omp_set_lock(&(lineQueue->lock));
            lineQueue->line.push(newLine);
            omp_unset_lock(&(lineQueue->lock));
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

/**
 * @brief Takes a word and a hash function and returns its desired reducer queue destination
 *
 * @param wordMap a reference to the word map to reduce
 * @param wordHashFn a reference to the hash function
 * @param maxReducers the max number of reducers used
 *
 * @return the reducer queue id number
 */
unsigned int getReducerQueueId(const std::string &word, const std::hash<std::string> &wordHashFn, const unsigned int maxReducers)
{
    unsigned int num = wordHashFn(word) % maxReducers;

    return num;
}

// void addTestFiles(const std::string &dirPath, std::queue<std::string> &testFiles)
// {
//     std::queue<std::string> testFilePaths;
//     std::string testPath = "../test/files";
//     for (const auto & entry : std::filesystem::directory_iterator(testPath))
//     {
//         testFiles.push(entry.path().string());
//     }
// }
    