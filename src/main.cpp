#include <iostream>

#include "MapReduce.h"

int main() {
    // master process keeps list of files

    // master process still performs map reduce
    mapReduceSerial();
    mapReduceParallel();

    // master process is notified when all processors have finished

    return 0;
}