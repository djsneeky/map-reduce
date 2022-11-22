#ifndef MAP_REDUCE_H_
#define MAP_REDUCE_H_

#include <map>
#include <string>
#include <cstdbool>
#include <queue>

void mapReduceParallel();
bool mapReduceSerial();
bool populateLineQueue(const std::string& fileName, std::queue<std::string>& lineQueue);
void populateWordMap(const std::string& line, std::map<std::string, int>& wordMap, char delim);

#endif