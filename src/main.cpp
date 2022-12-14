#include <iostream>

#include "MapReduce.h"
#include <fstream>
#include <sstream>
#include <mpi.h>

int main(int argc, char *argv[]) {
    // master process keeps list of files

    // master process still performs map reduce
    // "serial" run
    MPI_Init(&argc,&argv);


    std::vector<int> numberOfThreads;
    numberOfThreads.push_back(1);
    numberOfThreads.push_back(2);
    numberOfThreads.push_back(4);
    numberOfThreads.push_back(8);
    numberOfThreads.push_back(16);

    std::vector<int> numberOfRepeatFiles;
    numberOfRepeatFiles.push_back(1);
    numberOfRepeatFiles.push_back(5);
    numberOfRepeatFiles.push_back(10);
    numberOfRepeatFiles.push_back(15);
    numberOfRepeatFiles.push_back(20);

    std::ofstream ofs;
    ofs.open("build/results.txt", std::ofstream::out | std::ofstream::trunc);
    ofs.close();

    // for(auto repeat: numberOfRepeatFiles)
    // {
    //     for(auto item: numberOfThreads)
    //     {
    //         mapReduceParallel(item,item,item, "parallel" + std::to_string(item) + ".txt", repeat);
    //     }
    // }

    mapReduceMPIParallel(2,2,2,"memes.txt", 5);
    // mapReduceMPIParallel(1,1,1,"memes.txt", 5);



    // master process is notified when all processors have finished
    MPI_Finalize();

    return 0;
}