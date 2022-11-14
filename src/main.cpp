#include <iostream>

#include "MapReduce.h"

int main() {
    int num_procs = 0;
    int num_threads = 0;
    std::cout << "Map Reduce: " << num_procs << " procs, " << num_threads << " threads per proc" << std::endl;

    // master process keeps list of files

    // master process still performs map reduce
    mapReduce();

    // master process is notified when all processors have finished

    return 0;
}