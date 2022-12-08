#ifndef MAP_REDUCE_H_
#define MAP_REDUCE_H_

#include <map>
#include <string>
#include <cstdbool>
#include <queue>
#include <omp.h>


struct line_queue_t
{
    line_queue_t()
    : filled(false)
    {}
    std::vector<std::string> line;
    omp_lock_t lock;
    bool filled;
};

struct reducer_queue_t
{
    std::queue<std::pair<std::string, int>> wordQueue;
    omp_lock_t lock;
    bool filled;
};

void mapReduceParallel();
bool mapReduceSerial();
unsigned int getReducerQueueId(const std::string& word, const std::hash<std::string>& wordHashFn, const unsigned int maxReducers);
void readerTask(std::vector<std::string> &testFileList, std::vector<line_queue_t*>& lineQueues, volatile int *readersDone);
void mapperTask(std::vector<line_queue_t*>& lineQueues, 
                std::map<std::string, int> &wordMap, 
                std::vector<reducer_queue_t*> &reducerQueues,
                int reducerCount,
                const std::hash<std::string> &wordHashFn,
                volatile int *readersDone,
                volatile int *mappersDone);
void reducerTask(reducer_queue_t* reducerQueue, std::map<std::string, int> &reducerMap, volatile int* mappersDone, volatile int* reducersDone);
void writeOutFile(std::vector<std::map<std::string, int>> &reducerMaps, std::ofstream &output);
void writeOutFile(std::map<std::string, int> &reducerMaps, std::ofstream &output);
void writeOutReducersConsole(std::vector<std::map<std::string, int>> &reducerMaps);
bool populateLineQueues(const std::string &fileName, std::vector<std::queue<std::string>> &lineQueues);
bool populateLineQueue(const std::string& fileName, std::queue<std::string>& lineQueue);
bool populateLineQueue(const std::string &fileName, line_queue_t* lineQueue);
void populateWordMap(std::string& line, std::map<std::string, int>& wordMap);
void addTestFiles(const std::string &dirPath, std::queue<std::string> &testFiles);

#endif