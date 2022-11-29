#ifndef MAP_REDUCE_H_
#define MAP_REDUCE_H_

#include <map>
#include <string>
#include <cstdbool>
#include <queue>

void mapReduceParallel();
bool mapReduceSerial();
unsigned int getReducerQueueId(const std::string& word, const std::hash<std::string>& wordHashFn, const unsigned int maxReducers);
void readerTask(std::queue<std::string> testFileList, std::queue<std::string> lineQueue);
void mapperTask(std::queue<std::string> &lineQueue, 
                std::map<std::string, int> &wordMap, 
                std::vector<std::queue<std::pair<std::string, int>>> &reducerQueues,
                unsigned int reducerCount);
bool populateLineQueues(const std::string &fileName, std::vector<std::queue<std::string>> &lineQueues);
bool populateLineQueue(const std::string& fileName, std::queue<std::string>& lineQueue);
void populateWordMap(const std::string& line, std::map<std::string, int>& wordMap, char delim);
void addTestFiles(const std::string &dirPath, std::queue<std::string> &testFiles);

#endif