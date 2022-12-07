#ifndef MAP_REDUCE_H_
#define MAP_REDUCE_H_

#include <map>
#include <string>
#include <cstdbool>
#include <queue>
#include <omp.h>


struct line_queue_t
{
    std::vector<std::string> line;
    omp_lock_t lock;
};

struct reducer_queue_t
{
    std::queue<std::pair<std::string, int>> wordQueue;
    omp_lock_t lock;
};

void mapReduceParallel();
bool mapReduceSerial();
unsigned int getReducerQueueId(const std::string& word, const std::hash<std::string>& wordHashFn, const unsigned int maxReducers);
void readerTask(std::vector<std::string> &testFileList, line_queue_t* lineQueue);
void mapperTask(line_queue_t* lineQueue, 
                std::map<std::string, int> &wordMap, 
                std::vector<reducer_queue_t*> &reducerQueues,
                unsigned int reducerCount,
                const std::hash<std::string> &wordHashFn);
void reducerTask(reducer_queue_t* reducerQueue, std::map<std::string, int> &reducerMap);
void writerTask(std::map<std::string, int> &reducerMap, std::ofstream &output);
bool populateLineQueues(const std::string &fileName, std::vector<std::queue<std::string>> &lineQueues);
bool populateLineQueue(const std::string& fileName, std::queue<std::string>& lineQueue);
bool populateLineQueue(const std::string &fileName, line_queue_t* lineQueue);
void populateWordMap(std::string& line, std::map<std::string, int>& wordMap);
void addTestFiles(const std::string &dirPath, std::queue<std::string> &testFiles);

#endif