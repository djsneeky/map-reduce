#include "FileHelper.h"
// #include "filesystem"
#include <iostream>

#ifdef _WIN32
    #define WIN32_LEAN_AND_MEAN
    #define NOMINMAX
    #include <windows.h>
#else //OS_LINUX
    #include <limits.h>
    #include <unistd.h>
#endif

#include <dirent.h>
#include <sys/types.h>


std::string getExecutablePath()
{
#ifdef _WIN32
    char buffer[MAX_PATH + 1];
    int count = ::GetModuleFileNameA(NULL, buffer, MAX_PATH);
    return std::string(buffer, (count > 0) ? count : 0);
#else
    char buffer[PATH_MAX];
    ssize_t count = readlink("/proc/self/exe", buffer, PATH_MAX);
    return std::string(buffer, (count > 0) ? count : 0);
#endif
} 

std::vector<std::string> getListOfTestFiles(std::string dirPath, int repeatFile)
{
    std::vector<std::string> testFiles;

    // std::string directoryOfexe = std::filesystem::path{ getExecutablePath() }.parent_path().string();
    // const std::string pathToTestFile = dirPath;
    // if (pathToTestFile.empty())
    // {
    //     return testFiles;
    // }
    // std::string absPathToFiles = directoryOfexe + pathToTestFile;

    // std::cout << "Using " << absPathToFiles << " as root testing directory" << std::endl;
    
    // for (const auto & entry : std::filesystem::directory_iterator(absPathToFiles))
    // {
    //     for(int i = 0; i < repeatFile; ++i)
    //     {
    //         testFiles.push_back(entry.path());
    //     }
    // }




   DIR *dr;
   struct dirent *en;
   dr = opendir(dirPath.c_str()); //open all directory
   if (dr) {
      while ((en = readdir(dr)) != NULL) {
        std::string fileName(en->d_name);
        if(fileName.find(".txt") != std::string::npos)
        {
            for(int i = 0; i < repeatFile; ++i)
            {
                std::cout << dirPath+ std::string(en->d_name) << std::endl;
                testFiles.push_back(dirPath+ std::string(en->d_name));
            }
        }

      }
      closedir(dr); //close all directory
   }
    std::cout << "Using " << testFiles.size() << " files" << std::endl;

    return testFiles;
}

