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
#include <chrono>
#include <ctime>
#include <matplot/matplot.h>

#include "FileHelper.h"

omp_lock_t lineQueueLock;
omp_lock_t mapperDoneCount;


// openmp function that runs on each proc
void mapReduceParallel(int readerThreadCount, int mapperThreadCount, int reducerThreadCount, std::string outputFileName)
{

    int maxThreads = omp_get_max_threads();
    std::vector<std::string> testFiles = getListOfTestFiles("/../../test/files", 10);
    volatile bool finishedPopulatingLineQueues = false;
    // queues for lines - one for each mapper thread
    std::vector<line_queue_t*> lineQueues;
    omp_init_lock(&lineQueueLock);
    omp_init_lock(&mapperDoneCount);

    // word maps - one for each mapper thread
    std::vector<std::map<std::string, int>> wordMaps;
    for (int i = 0; i < maxThreads; i++)
    {
        wordMaps.push_back(std::map<std::string, int>());
    }

    // hash function for <string, int> pair reducer id
    const std::hash<std::string> wordHashFn;

    // create reducer queues
    std::vector<reducer_queue_t*> reducerQueues;
    for (int i = 0; i < maxThreads; i++)
    {
        reducer_queue_t* newReducerQueue = new reducer_queue_t;
        newReducerQueue->filled = false;
        omp_init_lock(&(newReducerQueue->lock));
        reducerQueues.push_back(newReducerQueue);
    }

    // create reducer maps
    std::vector<std::map<std::string, int>> reducerMaps;
    for (int i = 0; i < maxThreads; i++)
    {
        reducerMaps.push_back(std::map<std::string, int>());
    }

    std::cout << "Reader thread count " << readerThreadCount << std::endl;
    std::cout << "Mapper thread count: " << mapperThreadCount << std::endl;
    std::cout << "Reducer thread count: " << reducerThreadCount << std::endl;

    volatile int mappersDone = mapperThreadCount;
    volatile int readersDone = readerThreadCount;
    volatile int reducersDone = reducerThreadCount;
    
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<double> readerRunTimes;
    std::vector<double> mapperRunTimes;
    std::vector<double> reducerRunTimes;

    #pragma omp parallel
    {
        #pragma omp single
        {
            // Reader threads
            std::cout << "Creating readerTasks..." << std::endl;
            for (int i = 0; i < readerThreadCount; i++)
            {
                #pragma omp task
                {
                    // takes a file list and populates a thread specific line queue
                    double readerRunTime = readerTask(testFiles, lineQueues, &readersDone);
                    readerRunTimes.push_back(readerRunTime);
                }
            }
            // Mapper threads
            std::cout << "Creating mapperTasks..." << std::endl;
            for (int i = 0; i < mapperThreadCount; i++)
            {
                #pragma omp task 
                {
                    double mapperRunTime = mapperTask(lineQueues, wordMaps[i], reducerQueues, reducerThreadCount, wordHashFn, &readersDone, &mappersDone);
                    mapperRunTimes.push_back(mapperRunTime);
                }
            }
            // Reducer threads
            std::cout << "Creating reducerTasks..." << std::endl;
            for (int i = 0; i < reducerThreadCount; i++)
            {
                #pragma omp task
                {
                    double reducerRunTime = reducerTask(reducerQueues[i], reducerMaps[i], &mappersDone, &reducersDone);
                    reducerRunTimes.push_back(reducerRunTime);
                }
            }
        }
    }

    // create output file and lock
    std::ofstream outFile;
    outFile.open("build/" + outputFileName, std::ios::trunc | std::ios::out);
    std::cout << "Writing to output file..." << std::endl;
    writeOutFile(reducerMaps, outFile);
    outFile.close();
    std::cout << "Output file written" << std::endl;

    // writeOutReducersConsole(reducerMaps);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end-start;
    std::cout << "parallel elapsed_seconds: " << elapsed_seconds.count() << std::endl;

    // plots
    std::vector<std::string> threadLabel = {"Reader Threads", "Mapper Threads", "Reducer Threads"};
    std::vector<std::vector<double>> threadTimings = {readerRunTimes, mapperRunTimes, reducerRunTimes};

    matplot::show();
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
    std::map<std::string, int> wordMap;

    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::string> testFiles = getListOfTestFiles("/../../test/files", 10);
    while(!testFiles.empty())
    {
        std::queue<std::string> lineQueue;
        std::string file = testFiles.back();
        testFiles.pop_back();
        populateLineQueue(file, lineQueue);

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

    }

        // create output file and lock
    std::ofstream outFile;
    outFile.open("build/output_serial.txt", std::ios::trunc | std::ios::out);
    // clean the file
    std::cout << "Writing to output file..." << std::endl;
    writeOutFile(wordMap, outFile);
    outFile.close();
    std::cout << "Output file written" << std::endl;

    // Some computation here
    auto end = std::chrono::high_resolution_clock::now();
 
    std::chrono::duration<double> elapsed_seconds = end-start;
    std::cout << "serial elapsed_seconds: " << elapsed_seconds.count() << std::endl;
    return true;
}

/**
 * @brief Takes a file list and populates a thread's line queue
 * 
 * @param testFileList a list of file paths
 * @param lineQueue a pointer to the line queue
 */
double readerTask(std::vector<std::string> &testFileList, std::vector<line_queue_t*>& lineQueues, volatile int *readersDone)
{
    unsigned int filesRemaining;
    std::string testFile;
    line_queue_t* newQueue = nullptr;
    bool doneFlag = false;

    // reader start time
    auto start = std::chrono::high_resolution_clock::now();
    std::chrono::time_point<std::chrono::high_resolution_clock> end;
    std::chrono::duration<double> elapsed_seconds;

    while(true)
    {
        // std::cout << "reader" << std::endl;
        #pragma omp critical
        {
            filesRemaining = testFileList.size();
            if(filesRemaining == 0)
            {
                // std::cout << "readersDone: " << *readersDone << std::endl;
                *readersDone = *readersDone - 1;
                doneFlag = true;
            }
            else
            {
                testFile = testFileList.back();
                testFileList.pop_back();
                newQueue = new line_queue_t;
                omp_init_lock(&(newQueue->lock));
                newQueue->filled = false;

                omp_set_lock(&lineQueueLock);
                lineQueues.push_back(newQueue);
                omp_unset_lock(&lineQueueLock);
            }
        }
        if (doneFlag)
        {
            // reader end time
            end = std::chrono::high_resolution_clock::now();
            elapsed_seconds = end - start;
            std::cout << "Reader thread " << omp_get_thread_num() << " elapsed_seconds: " << elapsed_seconds.count() << std::endl;
            if (*readersDone == 0)
            {
                std::cout << "Reader threads completed" << std::endl;
            }
            break;
        }
        populateLineQueue(testFile, newQueue);
        omp_set_lock(&(newQueue->lock));
        newQueue->filled = true;
        omp_unset_lock(&(newQueue->lock));
    }

    return elapsed_seconds.count();
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
double mapperTask(std::vector<line_queue_t *> &lineQueues,
                  std::map<std::string, int> &wordMap,
                  std::vector<reducer_queue_t *> &reducerQueues,
                  int reducerCount,
                  const std::hash<std::string> &wordHashFn,
                  volatile int *readersDone,
                  volatile int *mappersDone)
{
    std::vector<std::map<std::string, int>> localMaps(reducerCount);

    // mapper start time
    auto start = std::chrono::high_resolution_clock::now();
    while(true)
    {
        if((lineQueues.size() == 0) && (*readersDone != 0))
        {
            #pragma omp taskyield
        }
        else if((lineQueues.size() == 0) && (*readersDone == 0))
        {
            break;
        }
        else
        {
            line_queue_t* lineQueue = nullptr;
            omp_set_lock(&lineQueueLock);
            if(lineQueues.size() != 0)
            {
                lineQueue = lineQueues.back();
                lineQueues.pop_back();
            }
            omp_unset_lock(&lineQueueLock);

            if(lineQueue)
            {
                while (!lineQueue->filled || (lineQueue->line.size() != 0) )
                {
                    std::vector<std::string> lineWork;
                    omp_set_lock(&(lineQueue->lock));
                    while(lineQueue->line.size() != 0)
                    {
                        lineWork.push_back(lineQueue->line.back());
                        lineQueue->line.pop_back();
                    }
                    omp_unset_lock(&(lineQueue->lock));
                    for(auto line: lineWork)
                    {
                        populateWordMap(line, localMaps, wordHashFn, reducerCount);
                    }
                }

                for(int i = 0; i < reducerCount; ++i)
                {
                    omp_set_lock(&(reducerQueues[i]->lock));
                    auto queue = reducerQueues[i]->wordQueue;
                    // std::cout << "pushing to queue i: " << i << std::endl;
                    for(auto pair: localMaps[i])
                    {
                        // std::cout << pair.first <<" " << pair.second << std::endl;
                        reducerQueues[i]->wordQueue.push(std::make_pair(pair.first, pair.second));

                    }
                    omp_unset_lock(&(reducerQueues[i]->lock));

                }

                for(int i = 0; i < reducerCount; ++i)
                {
                    localMaps[i].clear();
                }
            }



        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;

    #pragma omp critical
    {
        *mappersDone = *mappersDone - 1;
        
        std::cout << "Mapper thread " << omp_get_thread_num() << " elapsed_seconds: " << elapsed_seconds.count() << std::endl;
    }
    if (*mappersDone == 0)
    {
        std::cout << "Mapper threads completed" << std::endl;
    }

    return elapsed_seconds.count();
}

/**
 * @brief Takes pairs out of a reducer queue and adds them to a final reducer map
 * 
 * @param reducerQueue a pointer to the reducer queue of pairs
 * @param reducerMap a thread specific reducer map
 */
double reducerTask(reducer_queue_t *reducerQueue,
                   std::map<std::string, int> &reducerMap,
                   volatile int *mappersDone,
                   volatile int *reducersDone)
{
    // reducer start time
    auto start = std::chrono::high_resolution_clock::now();
    while(true)
    {
        omp_set_lock(&(reducerQueue->lock));
        if ((reducerQueue->wordQueue.size() == 0) && *mappersDone != 0)
        {
            omp_unset_lock(&(reducerQueue->lock));
            #pragma omp taskyield
        }
        else if ((reducerQueue->wordQueue.size() == 0) && *mappersDone == 0 )
        {
            omp_unset_lock(&(reducerQueue->lock));
            break;
        }
        else
        {
            if (reducerQueue->wordQueue.size() != 0)
            {
                auto pair = reducerQueue->wordQueue.front();
                reducerQueue->wordQueue.pop();
                // TODO: Evaluate a more efficient method to add to a reducer map
                // std::cout << pair.first << " "<< pair.second <<  std::endl;
                omp_unset_lock(&(reducerQueue->lock));
                auto ret = reducerMap.insert(pair);
                // if element already exists, add to the count
                if (ret.second == false)
                {
                    reducerMap[pair.first] += pair.second;
                }
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;

    #pragma omp critical
    {
        *reducersDone = *reducersDone - 1;
        std::cout << "Reducer thread " << omp_get_thread_num() << " elapsed_seconds: " << elapsed_seconds.count() << std::endl;
    }
    if (*reducersDone == 0)
    {
        std::cout << "Reducer threads completed" << std::endl;
    }

    return elapsed_seconds.count();
}

/**
 * @brief Takes a reducer map and writes it out to a file
 * 
 * @param reducerMaps a reference to a list of reducer maps
 * @param output output file stream
 */
void writeOutFile(std::vector<std::map<std::string, int>> &reducerMaps, std::ofstream &output)
{
    for (auto m : reducerMaps)
    {
        for (auto it = m.begin(); it != m.end(); it++)
        {
            output << it->first << " " << it->second << std::endl;
        }
    }
}

/**
 * @brief Write out a single reducer map to the console
 * 
 * @param reducerMaps a reference to a reducer map
 * @param output a reference to the output file stream object
 */
void writeOutFile(std::map<std::string, int> &reducerMaps, std::ofstream &output)
{
    for (auto it = reducerMaps.begin(); it != reducerMaps.end(); it++)
    {
        output << it->first << " " << it->second << std::endl;
    }
}

/**
 * @brief Takes all reducer maps and writes them out to the console
 * 
 * @param reducerMaps a reference to a list of reducer maps
 * @param output output file stream
 */
void writeOutReducersConsole(std::vector<std::map<std::string, int>> &reducerMaps)
{
    for (auto m : reducerMaps)
    {
        for (auto it = m.begin(); it != m.end(); it++)
        {
            std::cout << it->first << " " << it->second << std::endl;
        }
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
    const int LINES_PER_BLOCK_READER = 100;
    if (file.is_open())
    {
        while(!file.eof())
        {
            omp_set_lock(&(lineQueue->lock));
            for(int i = 0; i < LINES_PER_BLOCK_READER; ++i)
            {
                std::string newLine;
                if(std::getline(file, newLine))
                {
                    lineQueue->line.push_back(newLine);
                }
                else{
                    break;
                }
            }
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


void populateWordMap(std::string &line, std::vector<std::map<std::string, int>>& localMaps, const std::hash<std::string> &wordHashFn, int reducerCount)
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
                {
                    std::string word = line.substr(prev, pos-prev);
                    int reducerQueueId = getReducerQueueId(word, wordHashFn, reducerCount);

                    localMaps[reducerQueueId][word]++;
                }
                prev = pos+1;
            }
            if (prev < line.length())
            {
                std::string word = line.substr(prev, std::string::npos);
                int reducerQueueId = getReducerQueueId(word, wordHashFn, reducerCount);
                localMaps[reducerQueueId][word]++;
            }
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
    