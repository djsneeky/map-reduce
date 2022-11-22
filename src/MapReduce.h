#ifndef MAP_REDUCE_H_
#define MAP_REDUCE_H_

#include <map>
#include <string>

void mapReduce();
void populateLineQueue();
void populateWordMap(const std::string& line, std::map<std::string, int>& wordMap, char delim);

#endif