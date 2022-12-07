#include "MapReduce.h"

#include <map>
#include <queue>
#include <string>
#include <fstream>
#include <sstream>
#include <cstdbool>
#include <iostream>
#include <filesystem>
#include <regex>

#include "FileHelper.h"

// openmp function that runs on each proc
void mapReduceParallel()
{

    int max_threads = omp_get_max_threads();
    std::vector<std::string> testFiles = getListOfTestFiles();

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

    unsigned int readerThreadCount = omp_get_max_threads();
    unsigned int mapperThreadCount = omp_get_max_threads();
    unsigned int reducerThreadCount = omp_get_max_threads();

    #pragma omp parallel
    {
        int thread_id;
        // Reader threads
        #pragma omp single nowait
        {
            for (int i = 0; i < readerThreadCount; i++)
            {
                #pragma omp task
                {
                    thread_id = omp_get_thread_num();

                    // takes a file list and populates a thread specific line queue
                    readerTask(testFiles, lineQueues[thread_id]);
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
                    thread_id = omp_get_thread_num();
                    mapperTask(lineQueues[thread_id], wordMaps[thread_id], reducerQueues, reducerThreadCount, wordHashFn);
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
                    thread_id = omp_get_thread_num();
                    reducerTask(reducerQueues[thread_id], reducerMaps[thread_id]);
                }
            }
        }
    }

    // create output file and lock
    std::ofstream outFile;
    outFile.open("build/output.txt", std::ios::trunc | std::ios::out);
    // clean the file
    std::cout << "Writing to output file..." << std::endl;

    // TODO: output all pairs to a file
    #pragma omp parallel
    {
        int thread_id;
        #pragma omp single
        {
            for (int i = 0; i < reducerThreadCount; i++)
            {
                #pragma omp task
                {
                    thread_id = omp_get_thread_num();
                    writerTask(reducerMaps[thread_id], outFile);
                }
            }
        }
    }

    outFile.close();
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
    if (!populateLineQueue("../test/files/16.txt", lineQueue))
    {
        return false;
    }

    // read lines out of queue and populate a map
    while (!lineQueue.empty())
    {
        populateWordMap(lineQueue.front(), wordMap);
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
 * @brief Takes a file list and populates a thread's line queue
 * 
 * @param testFileList a list of file paths
 * @param lineQueue a pointer to the line queue
 */
void readerTask(std::vector<std::string> &testFileList, line_queue_t* lineQueue)
{
    unsigned int filesRemaining;
    std::string testFile;
    #pragma omp critical
    filesRemaining = testFileList.size();
    while (filesRemaining != 0)
    {
        #pragma omp critical
        {
            filesRemaining = testFileList.size();
            if(filesRemaining != 0)
            {
                testFile = testFileList.back();
                testFileList.pop_back();
            }
        }
        populateLineQueue(testFile, lineQueue);
    }
}

/**
 * @brief Map threads read line queues and place pairs into a local word map.
 *        Then the thread will place hashed word pairs onto reducer queues.
 * 
 * @param lineQueue a pointer to the line queue
 * @param wordMap a thread specific word map
 * @param reducerQueues a reference to a list of reducer queues
 * @param reducerCount the max number of reducer threads
 * @param wordHashFn a hash function used to map words to reducer queues
 */
void mapperTask(line_queue_t* lineQueue, 
                std::map<std::string, int> &wordMap, 
                std::vector<reducer_queue_t*> &reducerQueues,
                unsigned int reducerCount,
                const std::hash<std::string> &wordHashFn)
{
    // check to see that there are lines available in the queue
    std::string line;
    omp_set_lock(&(lineQueue->lock));
    unsigned int linesRemaining = lineQueue->line.size();
    // std::cout << linesRemaining << std::endl;
    omp_unset_lock(&(lineQueue->lock));
    while (linesRemaining != 0)
    {
        std::string line;
        // map thread to read line queues and place words into thread local word map
        omp_set_lock(&(lineQueue->lock));
        linesRemaining = lineQueue->line.size();
        if(linesRemaining != 0) 
        {
            line = lineQueue->line.back();
            lineQueue->line.pop_back();
        }
        omp_unset_lock(&(lineQueue->lock));

        populateWordMap(line, wordMap);
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
    }
    // clear the word map
    wordMap.clear();
}

/**
 * @brief Takes pairs out of a reducer queue and adds them to a final reducer map
 * 
 * @param reducerQueue a pointer to the reducer queue of pairs
 * @param reducerMap a thread specific reducer map
 */
void reducerTask(reducer_queue_t* reducerQueue, std::map<std::string, int> &reducerMap)
{
    std::pair<std::string, int> pair;
    std::pair<std::map<std::string, int>::iterator, bool> ret;
    // TODO: minimize locking on reducer queue
    omp_set_lock(&(reducerQueue->lock));
    while (!reducerQueue->wordQueue.empty())
    {
        pair = reducerQueue->wordQueue.front();
        reducerQueue->wordQueue.pop();
        // TODO: Evaluate a more efficient method to add to a reducer map
        ret = reducerMap.insert(pair);
        // if element already exists, add to the count
        if (ret.second = false)
        {
            reducerMap[pair.first] += pair.second;
        }
    }
    omp_unset_lock(&(reducerQueue->lock));
}

/**
 * @brief Takes a reducer map and writes it out to a file
 * 
 * @param reducerMap a reference to a reducer map
 * 
 */
void writerTask(std::map<std::string, int> &reducerMap, std::ofstream &output)
{
    // TODO: Consider using a single queue to write to output file to reduce contention
    for (auto it = reducerMap.begin(); it != reducerMap.end(); it++)
    {
        #pragma omp critical
        output << it->first << " " << it->second << std::endl;
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
bool populateLineQueue(const std::string &fileName, line_queue_t* lineQueue)
{
    std::ifstream file(fileName);
    if (file.is_open())
    {
        std::string newLine;
        while (std::getline(file, newLine))
        {
            omp_set_lock(&(lineQueue->lock));
            lineQueue->line.push_back(newLine);
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
 * @brief Reads a file line by line and populates a line queue
 *
 * Used by the reader threads
 *
 * @param fileName the absolute path of the file
 * @param lineQueue the queue to populate
 * @return true if the file has been processed successfully, false otherwise
 */
bool populateLineQueue(const std::string& fileName, std::queue<std::string>& lineQueue)
{
    std::ifstream file(fileName);
    int counter = 0;
    if (file.is_open())
    {
        std::string newLine;
        while (std::getline(file, newLine))
        {
            lineQueue.push(newLine);
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
 */
void populateWordMap(std::string &line, std::map<std::string, int> &wordMap)
{
    if (line.size() != 0)
    {
        std::stringstream stringStream(line);
        while(std::getline(stringStream, line))
        {
            std::size_t prev = 0, pos;
            while ((pos = line.find_first_of(" ';:,-<>.\"!?_*", prev)) != std::string::npos)
            {
                if (pos > prev)
                    wordMap[line.substr(prev, pos-prev)]++;
                prev = pos+1;
            }
            if (prev < line.length())
                wordMap[line.substr(prev, std::string::npos)]++;
        }
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
    