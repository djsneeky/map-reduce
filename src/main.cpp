#include <iostream>

#include "MapReduce.h"

int main() {
    // master process keeps list of files

    // master process still performs map reduce
    // "serial" run

    std::vector<int> numberOfThreads;
    numberOfThreads.push_back(1);
    numberOfThreads.push_back(2);
    numberOfThreads.push_back(4);
    numberOfThreads.push_back(8);
    numberOfThreads.push_back(16);

    for(auto item: numberOfThreads)
    {
        mapReduceParallel(item,item,item, "parallel" + std::to_string(item) + ".txt");
    }

    // mapReduceParallel(1,1,1, "serial.txt");
    // mapReduceParallel(2,2,2, "parallel2.txt");
    // mapReduceParallel(4,4,4, "parallel4.txt");
    // mapReduceParallel(8,8,8, "parallel2.txt");
    // mapReduceParallel(16,16,16, "parallel16.txt");


    // master process is notified when all processors have finished

    return 0;
}