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
#include <numeric>
#include <matplot/matplot.h>
#include <mpi.h>

#include "FileHelper.h"

omp_lock_t lineQueueLock;
omp_lock_t mapperDoneCount;
omp_lock_t sendBufferLock;
omp_lock_t localFileListLock;


// openmp function that runs on each proc
void mapReduceParallel(int readerThreadCount, int mapperThreadCount, int reducerThreadCount, std::string outputFileName, int repeatFiles)
{

    int maxThreads = omp_get_max_threads();
    std::vector<std::string> testFiles = getListOfTestFiles("/../../test/files", repeatFiles);
    int numberOfFiles = testFiles.size();
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
                    #pragma omp critical
                    {
                        readerRunTimes.push_back(readerRunTime);
                    }
                }
            }
            // Mapper threads
            std::cout << "Creating mapperTasks..." << std::endl;
            for (int i = 0; i < mapperThreadCount; i++)
            {
                #pragma omp task 
                {
                    double mapperRunTime = mapperTask(lineQueues, wordMaps[i], reducerQueues, reducerThreadCount, wordHashFn, &readersDone, &mappersDone);
                    #pragma omp critical
                    {
                        mapperRunTimes.push_back(mapperRunTime);    
                    }
                }
            }
            // Reducer threads
            std::cout << "Creating reducerTasks..." << std::endl;
            for (int i = 0; i < reducerThreadCount; i++)
            {
                #pragma omp task
                {
                    double reducerRunTime = reducerTask(reducerQueues[i], reducerMaps[i], &mappersDone, &reducersDone);
                    #pragma omp critical
                    {
                        reducerRunTimes.push_back(reducerRunTime);
                    }
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
    std::vector<std::string> threadLabel = {"ReaderThreads", "MapperThreads", "ReducerThreads"};
    std::vector<std::vector<double>> threadTimings = {readerRunTimes, mapperRunTimes, reducerRunTimes};

    std::ofstream outfileResults;
    outfileResults.open("build/results.txt", std::ios_base::app); // append instead of overwrite

    outfileResults << "Reader thread count " << readerThreadCount << std::endl;
    outfileResults << "Mapper thread count: " << mapperThreadCount << std::endl;
    outfileResults << "Reducer thread count: " << reducerThreadCount << std::endl;
    outfileResults << "Number of files Processed: " << numberOfFiles << std::endl;

    for(int i = 0; i < threadLabel.size(); ++i)
    {
        outfileResults << threadLabel[i] << " Times:" << std::endl;
        for(int j = 0; j < threadTimings[i].size(); ++j)
        {
            outfileResults << threadTimings[i][j] << " " << std::endl;
        }
        outfileResults << "\n";
    }


    outfileResults << "Total time spent executing: " << elapsed_seconds.count() << std::endl;
    outfileResults << "\n\n" << std::endl;

    outfileResults.close();
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
    // std::cout << "serial elapsed_seconds: " << elapsed_seconds.count() << std::endl;
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
            auto end = std::chrono::high_resolution_clock::now();
            elapsed_seconds = end - start;
            // std::cout << "Reader thread " << omp_get_thread_num() << " elapsed_seconds: " << elapsed_seconds.count() << std::endl;
            if (*readersDone == 0)
            {
                // std::cout << "Reader threads completed" << std::endl;
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
        
        // std::cout << "Mapper thread " << omp_get_thread_num() << " elapsed_seconds: " << elapsed_seconds.count() << std::endl;
    }
    if (*mappersDone == 0)
    {
        // std::cout << "Mapper threads completed" << std::endl;
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
        // std::cout << "Reducer thread " << omp_get_thread_num() << " elapsed_seconds: " << elapsed_seconds.count() << std::endl;
    }
    if (*reducersDone == 0)
    {
        // std::cout << "Reducer threads completed" << std::endl;
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


volatile mpiState state;
std::vector<SendBufferEntry> sendBuffer;
int size;
int rank;
int MessageHeader::pack(char* buffer)
{
    int offset = 0;

    memcpy(&buffer[offset], &messageId, sizeof(messageId));
    offset += sizeof(messageId);

    memcpy(&buffer[offset], &source, sizeof(source));
    offset += sizeof(source);

    return offset;
}

int MessageHeader::unpack(char* buffer)
{
    int offset = 0;
    memcpy(&messageId, &buffer[offset], sizeof(messageId));
    offset += sizeof(messageId);

    memcpy(&source, &buffer[offset], sizeof(source));
    offset += sizeof(source);
    return offset;
}

const int MessageHeader::getSize()
{
    return sizeof(messageId) + sizeof(source);
}

void processReceivedMessage(char* bufferReceived, int bufferReceivedSize,  std::vector<std::string>& testFileList, std::vector<std::string>& localFileList,  std::vector<reducer_queue_t*>& reducerQueues)
{
    MessageHeader header;
    int bufferReceivedOffset = header.unpack(bufferReceived);
    // std::cout << "unpacked message" << std::endl;
    // std::cout << "header.messageId: " << header.messageId << std::endl;
    // std::cout << "header.source: " << header.source << std::endl;
    switch(header.messageId)
    {
        case REQUEST_NEW_FILE:
        {
            // master process should only be getting this message
            SendBufferEntry entry;
            if(testFileList.size())
            {
                // std::cout << "sending new file" << std::endl;
                std::string file =  testFileList.back();
                testFileList.pop_back();

                MessageHeader newMessageHeader;
                newMessageHeader.messageId = NEW_FILE;
                newMessageHeader.source = 0; // master

                int fileNameSize = file.size() + 1;
                int bufferSize = fileNameSize + newMessageHeader.getSize();
                char* messageBuffer = new char[bufferSize];
                
                int messageOffset = newMessageHeader.pack(messageBuffer);

                memcpy(&messageBuffer[messageOffset], file.c_str(), fileNameSize);


                entry.buffer = messageBuffer;
                entry.size   = bufferSize;
                entry.dest   = header.source;
            }
            else
            {
                MessageHeader newMessageHeader;
                newMessageHeader.messageId = NO_MORE_FILES;
                newMessageHeader.source = 0;

                int messageSize = newMessageHeader.getSize();
                char* requestFileBuffer = new char[messageSize];
                newMessageHeader.pack(requestFileBuffer);

                entry.dest = header.source;
                entry.buffer = requestFileBuffer;
                entry.size = messageSize;
                state.outOfFiles = true;
                state.outOfFileSentMasterList[entry.dest] = true;
                state.outOfFileSentMaster++;
            }
            omp_set_lock(&sendBufferLock);
            sendBuffer.push_back(entry);
            omp_unset_lock(&sendBufferLock);

            break;
        }
        case NEW_FILE:
        {
            std::string newFile(&bufferReceived[bufferReceivedOffset]);
            // std::cout << "got new file: " << newFile << std::endl;
            omp_set_lock(&localFileListLock);
            localFileList.push_back(newFile);
            omp_unset_lock(&localFileListLock);
            state.messageSent = false;
            break;
        }
        case NO_MORE_FILES:
        {
            // std::cout << "rank: " << rank << " out of files" << std::endl;
            state.outOfFiles = true;
            break;
        }
        case REDUCER_QUEUE:
        {
            int queueId = *reinterpret_cast<int*>(&bufferReceived[bufferReceivedOffset]);
            bufferReceivedOffset += sizeof(queueId);
            std::map<std::string, int> map;
            unserializeMap(&bufferReceived[bufferReceivedOffset], bufferReceivedSize - bufferReceivedOffset, map);
            // unSerializeMap(&bufferReceived[bufferReceivedOffset], map);
            omp_set_lock(&(reducerQueues[queueId]->lock));
            auto queue = reducerQueues[queueId]->wordQueue;
            // std::cout << "pushing to queue i: " << i << std::endl;
            for(auto pair: map)
            {
                // std::cout << pair.first <<" " << pair.second << std::endl;
                reducerQueues[queueId]->wordQueue.push(std::make_pair(pair.first, pair.second));

            }
            omp_unset_lock(&(reducerQueues[queueId]->lock));
            break;
        }
        case MAPPER_DONE:
        {
            state.mappersDoneReceived++;
            // std::cout << "mappersDoneReceived: " << state.mappersDoneReceived << std::endl;
            break;
        }
        case ALL_MAPPERS_DONE:
        {
            state.allMappersDone = true;
            break;
        }
        default:
        {
            break;
        }
    }

}

bool noMoreFilesSentToAllNodes()
{
    return state.outOfFileSentMaster == size;
}

bool allMappersDone()
{
    return (state.mappersDoneReceived == (size - 1)) && state.masterMapperDone;
}

void sendAllMappersDone()
{
    SendBufferEntry entry;
    omp_set_lock(&sendBufferLock);
    for(int i = 1; i < size; ++i)
    {
        MessageHeader newMessageHeader;
        newMessageHeader.messageId = ALL_MAPPERS_DONE;
        newMessageHeader.source = 0;

        int messageSize = newMessageHeader.getSize();
        char* requestFileBuffer = new char[messageSize];
        newMessageHeader.pack(requestFileBuffer);

        entry.dest = i;
        entry.buffer = requestFileBuffer;
        entry.size = messageSize;
        sendBuffer.push_back(entry);
    }
    state.mappersDoneReceived = 0;
    state.allMappersDone = true;
    omp_unset_lock(&sendBufferLock);
}

void sendNoMoreFilesToNodes()
{
    SendBufferEntry entry;
    omp_set_lock(&sendBufferLock);
    for(int i = 1; i < size; ++i)
    {
        if(!state.outOfFileSentMasterList[i])
        {
            MessageHeader newMessageHeader;
            newMessageHeader.messageId = NO_MORE_FILES;
            newMessageHeader.source = 0;

            int messageSize = newMessageHeader.getSize();
            char* requestFileBuffer = new char[messageSize];
            newMessageHeader.pack(requestFileBuffer);

            entry.dest = i;
            entry.buffer = requestFileBuffer;
            entry.size = messageSize;
            state.outOfFileSentMasterList[i] = true;
            sendBuffer.push_back(entry);
        }
    }
    state.outOfFileSentMaster = size;
    omp_unset_lock(&sendBufferLock);
}


void mpiCommThread(std::vector<std::string>& testFileList,  std::vector<std::string>& localFileList, std::vector<reducer_queue_t*>& reducerQueues)
{
    bool messageSent = false;
    while(1)
    {
        MPI_Status status;
        omp_set_lock(&sendBufferLock);
        while(sendBuffer.size())
        {
            SendBufferEntry entry = sendBuffer.back();
            sendBuffer.pop_back();
            MessageHeader header;
            header.unpack(entry.buffer);
            // std::cout << "sending message type: " << header.messageId << " dest: " <<entry.dest  << " rank: " << rank << std::endl;
            MPI_Send(entry.buffer, entry.size, MPI_CHAR, entry.dest, 0, MPI_COMM_WORLD);
            delete [] entry.buffer;
        }
        omp_unset_lock(&sendBufferLock);
        
        int flag = 0;
        MPI_Iprobe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &flag, &status);

        if(flag == 1)
        {
            // std::cout << " message avilable rank: " << rank << std::endl;
            int bytesInMessage = 0;
            MPI_Get_count(&status, MPI_CHAR, &bytesInMessage);
            // std::cout << "bytesInMessage: " << bytesInMessage << std::endl;
            char* bufferReceived = new char[bytesInMessage];
            MPI_Recv(bufferReceived, bytesInMessage, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
            processReceivedMessage(bufferReceived, bytesInMessage, testFileList, localFileList, reducerQueues);
            delete [] bufferReceived;
        }

        // check if we need more files
        omp_set_lock(&localFileListLock);
        if (localFileList.size() == 0 && !state.messageSent && !state.outOfFiles)
        {
            if(rank != 0)
            {
                // std::cout << "Getting more files" << std::endl;
                MessageHeader newMessageHeader;
                newMessageHeader.messageId = REQUEST_NEW_FILE;
                newMessageHeader.source = rank;

                int messageSize = newMessageHeader.getSize();
                char* requestFileBuffer = new char[messageSize];
                newMessageHeader.pack(requestFileBuffer);

                SendBufferEntry entry;
                entry.dest = 0;
                entry.buffer = requestFileBuffer;
                entry.size = messageSize;

                omp_set_lock(&sendBufferLock);
                sendBuffer.push_back(entry);
                omp_unset_lock(&sendBufferLock);
                state.messageSent = true;
            }
            else
            {
                if(testFileList.size())
                {

                    std::string newFile = testFileList.back();
                    testFileList.pop_back();
                    localFileList.push_back(newFile);
                }
                else
                {
                    state.outOfFiles = true;
                }

            }
        }
        omp_unset_lock(&localFileListLock);

        if(allMappersDone())
        {
            sendAllMappersDone();
        }

        if(state.outOfFiles && (sendBuffer.size() == 0) && state.allMappersDone)
        {
            if(rank == 0)
            {
                if(noMoreFilesSentToAllNodes())
                {
                    // std::cout << "leaving comm rank: " << rank << std::endl;
                    break;
                }
                else
                {
                    // std::cout << "sending to all nodes" << std::endl;
                    sendNoMoreFilesToNodes();
                }
            }
            else
            {
                // std::cout << "leaving comm rank: " << rank << std::endl;
                break;
            }
        }
        else
        {
            #pragma omp taskyield
        }
    }
}

int getMapSize(std::map<std::string,int>& map)
{
    int size = 0;
    // need to find size of all the strings
    for(auto entry: map)
    {
        size += entry.first.length() + 1 + sizeof(entry.second);
    }
    return size;

}


void serializeMap(char* buffer, std::map<std::string,int>& map)
{
    int offset = 0;

    //serialize map
    for(auto entry: map)
    {   
        memcpy(&buffer[offset], entry.first.c_str(), entry.first.length() + 1);
        offset += entry.first.length() + 1;
        memcpy(&buffer[offset], &entry.second, sizeof(entry.second));
        offset += sizeof(entry.second);
    }
}

void unserializeMap(char* buffer, int bufferLength, std::map<std::string,int>& map)
{
    int offset = 0;
    while(offset < bufferLength)
    {
        std::string newString(&buffer[offset]);
        offset += newString.length() + 1;
        int newInt = *reinterpret_cast<int*>(&buffer[offset]);
        offset += sizeof(newInt);
        map.insert(std::make_pair(newString,newInt));
    }
}




void mapReduceMPIParallel(int readerThreadCount, int mapperThreadCount, int reducerThreadCount, std::string outputFileName, int repeatFiles)
{
    size = 0;
    rank = 0;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Status status;
    int maxThreads = omp_get_max_threads();
    std::cout << "size: " << size << std::endl;
    std::cout << "rank: " << rank << std::endl;
    std::cout << "max threads: " << maxThreads << std::endl;
    std::vector<std::string> testFiles = getListOfTestFiles("/../../test/files", repeatFiles);
    std::vector<std::string> localFiles;
    omp_init_lock(&sendBufferLock);
    omp_init_lock(&localFileListLock);

    state.messageSent = false;
    state.outOfFiles = false;
    state.allMappersDone = false;
    state.mappersDoneReceived = 0;
    state.outOfFileSentMaster = 0;
    for(int i = 0; i < MAX_NODES; ++i)
    {
        state.mappersDoneReceivedList[i] = false;
        state.outOfFileSentMasterList[i] = false;
    }

    volatile int mappersDone = mapperThreadCount;
    volatile int readersDone = readerThreadCount;
    volatile int reducersDone = reducerThreadCount;
    
    std::vector<double> readerRunTimes;
    std::vector<double> mapperRunTimes;
    std::vector<double> reducerRunTimes;

    // hash function for <string, int> pair reducer id
    const std::hash<std::string> wordHashFn;

    MPI_Barrier(MPI_COMM_WORLD);
    double elapsed = -MPI_Wtime();

    std::vector<reducer_queue_t*> reducerQueues;
    for (int i = 0; i < maxThreads; i++)
    {
        reducer_queue_t* newReducerQueue = new reducer_queue_t;
        newReducerQueue->filled = false;
        omp_init_lock(&(newReducerQueue->lock));
        reducerQueues.push_back(newReducerQueue);
    }

    std::vector<std::map<std::string, int>> reducerMaps;
    for (int i = 0; i < maxThreads; i++)
    {
        reducerMaps.push_back(std::map<std::string, int>());
    }

    std::vector<line_queue_t*> lineQueues;

    if(rank == 0)
    {
        std::string file;
        for (int i  = 1; i < size; ++i)
        {
            // assume there will always be enough files to distribute at least one for each process
            file =  testFiles.back();
            testFiles.pop_back();
            MPI_Send(file.c_str(),file.size() +1, MPI_CHAR, i, 0, MPI_COMM_WORLD);
        }
        file =  testFiles.back();
        testFiles.pop_back();
        localFiles.push_back(file);
    }
    else
    {
        char buff[100];
        MPI_Recv(buff, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
        std::string newFile(buff);
        // std::cout << "got file: " << newFile  << std::endl;
        localFiles.push_back(newFile);
    }


    #pragma omp parallel
    {
        #pragma omp single
        {
            
            #pragma omp task
            {
                mpiCommThread(testFiles, localFiles,reducerQueues) ;
            }
            std::cout << "Creating readerTasks..." << std::endl;
            for (int i = 0; i < readerThreadCount; i++)
            {
                #pragma omp task
                {
                    // takes a file list and populates a thread specific line queue
                    double readerRunTime = readerTaskMpi(localFiles, lineQueues, &readersDone);
                    #pragma omp critical
                    {
                        readerRunTimes.push_back(readerRunTime);
                    }
                }
            }

            std::cout << "Creating mapperTasks..." << std::endl;
            for (int i = 0; i < mapperThreadCount; i++)
            {
                #pragma omp task 
                {
                    double mapperRunTime = mapperTaskMpi(lineQueues, reducerQueues, reducerThreadCount, wordHashFn, &readersDone, &mappersDone);
                    #pragma omp critical
                    {
                        mapperRunTimes.push_back(mapperRunTime);    
                    }
                }
            }

            std::cout << "Creating reducerTasks..." << std::endl;
            for (int i = 0; i < reducerThreadCount; i++)
            {
                #pragma omp task
                {
                    double reducerRunTime = reducerTaskMpi(reducerQueues[i], reducerMaps[i], &state.allMappersDone, &reducersDone);
                    #pragma omp critical
                    {
                        reducerRunTimes.push_back(reducerRunTime);
                    }
                }
            }
        }

    }

     // create output file and lock
    std::ofstream outFile;
    outFile.open("build/" + std::to_string(rank) + outputFileName, std::ios::trunc | std::ios::out);
    std::cout << "Writing to output file..." << std::endl;
    writeOutFile(reducerMaps, outFile);
    outFile.close();
    std::cout << "Output file written" << std::endl;

    MPI_Barrier(MPI_COMM_WORLD);

    elapsed += MPI_Wtime( );
    if(rank == 0 )
    {
        std::cout << "elapsed: " << elapsed << std::endl;
    }
}

double readerTaskMpi(std::vector<std::string> &testFileList, std::vector<line_queue_t *> &lineQueues, volatile int *readersDone)
{
    unsigned int filesRemaining;
    std::string testFile;

    // reader start time
    auto elapsed = -MPI_Wtime();
    while(true)
    {
        // std::cout << "reader" << std::endl;
        line_queue_t* newQueue = nullptr;
        omp_set_lock(&localFileListLock);
        filesRemaining = testFileList.size();
        if(filesRemaining == 0 && state.outOfFiles)
        {
            // std::cout << "readersDone: " << *readersDone << std::endl;
            *readersDone = *readersDone - 1;
            omp_unset_lock(&localFileListLock);
            // reader end time
            elapsed += MPI_Wtime( );
            // std::cout << "Reader thread " << omp_get_thread_num() << " elapsed_seconds: " << elapsed << std::endl;
            if (*readersDone == 0)
            {
                // std::cout << "Reader threads completed rank: " <<rank << std::endl;
            }

            break;
        }
        else if(filesRemaining)
        {
            // std::cout << "getting file from queue: " << std::endl; 

            testFile = testFileList.back();
            testFileList.pop_back();
            omp_unset_lock(&localFileListLock);
            // std::cout << "got file from queue: " << testFile << " rank: " << rank << std::endl; 
            newQueue = new line_queue_t;
            omp_init_lock(&(newQueue->lock));
            newQueue->filled = false;

            omp_set_lock(&lineQueueLock);
            lineQueues.push_back(newQueue);
            omp_unset_lock(&lineQueueLock);

            
            // std::cout << "populating rank: " << rank  << std::endl;
            populateLineQueue(testFile, newQueue);
            // std::cout << "populating done rank: " << rank << std::endl;

            // std::cout << " fileRemaining " << filesRemaining << std::endl;

            omp_set_lock(&(newQueue->lock));
            newQueue->filled = true;
            omp_unset_lock(&(newQueue->lock));
        }
        else
        {
            omp_unset_lock(&localFileListLock);
            #pragma omp taskyield
        }

    }

    return elapsed;
}

double mapperTaskMpi(std::vector<line_queue_t *> &lineQueues,
                  std::vector<reducer_queue_t *> &reducerQueues,
                  int reducerCount,
                  const std::hash<std::string> &wordHashFn,
                  volatile int *readersDone,
                  volatile int *mappersDone)
{
    std::vector<std::vector<std::map<std::string, int>>> localMaps( size , std::vector<std::map<std::string, int>> (reducerCount));
    // mapper start time
    auto elapsed = -MPI_Wtime();

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
                        populateWordMapMpi(line, localMaps, wordHashFn, reducerCount);
                    }
                }

                for(int i = 0; i < size; ++i)
                {
                    for(int j = 0; j < reducerCount; ++j)
                    {
                        std::map<std::string, int> mapForNodeij = localMaps[i][j];
                        if(rank != i)
                        {
                            MessageHeader messageHeader;
                            messageHeader.messageId = REDUCER_QUEUE;
                            messageHeader.source = rank;
                            int bufferSize = getMapSize(mapForNodeij) + sizeof(j) + messageHeader.getSize();

                            char* buffer = new char[bufferSize];
                            int offset = messageHeader.pack(buffer);



                            *reinterpret_cast<int*>(&buffer[offset]) = j;
                            offset += sizeof(j);
                            serializeMap(&buffer[offset], mapForNodeij);


                            SendBufferEntry entry;
                            entry.buffer = buffer;
                            entry.dest = i;
                            entry.size = bufferSize;

                            omp_set_lock(&sendBufferLock);
                            sendBuffer.push_back(entry);
                            omp_unset_lock(&sendBufferLock);
                        }
                        else
                        {
                            omp_set_lock(&(reducerQueues[j]->lock));
                            auto queue = reducerQueues[j]->wordQueue;
                            // std::cout << "pushing to queue i: " << i << std::endl;
                            for(auto pair: mapForNodeij)
                            {
                                // std::cout << pair.first <<" " << pair.second << std::endl;
                                reducerQueues[j]->wordQueue.push(std::make_pair(pair.first, pair.second));

                            }
                            omp_unset_lock(&(reducerQueues[j]->lock));
                        }
                    }
                }

                for(auto& nodeQueue: localMaps)
                {
                    for(auto& nodeMap: nodeQueue)
                    {
                        nodeMap.clear();
                    }
                }
            }
        }
    }

    elapsed += MPI_Wtime( );


    #pragma omp critical
    {
        *mappersDone = *mappersDone - 1;
        
        // std::cout << "Mapper thread " << omp_get_thread_num() << " elapsed_seconds: " << elapsed_seconds.count() << std::endl;
    }
    if (*mappersDone == 0)
    {
        // std::cout << "Mapper threads completed rank: " << rank << std::endl;

        if(rank != 0)
        {

            SendBufferEntry entry;

            MessageHeader newMessageHeader;
            newMessageHeader.messageId = MAPPER_DONE;
            newMessageHeader.source = rank;

            int messageSize = newMessageHeader.getSize();
            char* requestFileBuffer = new char[messageSize];
            newMessageHeader.pack(requestFileBuffer);

            entry.dest = 0;
            entry.buffer = requestFileBuffer;
            entry.size = messageSize;


            omp_set_lock(&sendBufferLock);
            sendBuffer.push_back(entry);
            omp_unset_lock(&sendBufferLock);
        }
        else
        {
            state.masterMapperDone = true;
        }

    }

    return elapsed;
}

double reducerTaskMpi(reducer_queue_t *reducerQueue,
                      std::map<std::string, int> &reducerMap,
                      volatile bool *mappersDone,
                      volatile int *reducersDone)
{
    // reducer start time
    auto elapsed = -MPI_Wtime();
    while(true)
    {
        omp_set_lock(&(reducerQueue->lock));
        if ((reducerQueue->wordQueue.size() == 0) && !state.allMappersDone)
        {
            omp_unset_lock(&(reducerQueue->lock));
            #pragma omp taskyield
        }
        else if ((reducerQueue->wordQueue.size() == 0) && state.allMappersDone )
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

    elapsed += MPI_Wtime();


    #pragma omp critical
    {
        *reducersDone = *reducersDone - 1;
        // std::cout << "Reducer thread " << omp_get_thread_num() << " elapsed_seconds: " << elapsed_seconds.count() << std::endl;
    }
    if (*reducersDone == 0)
    {
        // std::cout << "Reducer threads completed rank: " << rank << std::endl;
    }

    return elapsed;
}


void populateWordMapMpi(std::string &line,
                        std::vector<std::vector<std::map<std::string, int>>>& localMaps,
                        const std::hash<std::string> &wordHashFn,
                        int reducerCount)
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
                    int nodeId = getReducerQueueId(word, wordHashFn, size);
                    int reducerQueueId = getReducerQueueId(word, wordHashFn, reducerCount);

                    localMaps[nodeId][reducerQueueId][word]++;
                }
                prev = pos+1;
            }
            if (prev < line.length())
            {
                std::string word = line.substr(prev, std::string::npos);
                int nodeId = getReducerQueueId(word, wordHashFn, size);
                int reducerQueueId = getReducerQueueId(word, wordHashFn, reducerCount);
                localMaps[nodeId][reducerQueueId][word]++;
            }
        }
    }
}
