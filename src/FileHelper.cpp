#include "FileHelper.h"
#include "filesystem"
#include <iostream>

#ifdef _WIN32
    #define WIN32_LEAN_AND_MEAN
    #define NOMINMAX
    #include <windows.h>
#else //OS_LINUX
    #include <limits.h>
    #include <unistd.h>
#endif


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

std::vector<std::string> getListOfTestFiles()
{
    std::vector<std::string> testFiles;

    std::string directoryOfexe = std::filesystem::path{ getExecutablePath() }.parent_path().string();
    const std::string pathToTestFile = "/../../test/files";
    std::string absPathToFiles = directoryOfexe + pathToTestFile;

    std::cout << "Using " << absPathToFiles << " as root testing directory" << std::endl;
    
    for (const auto & entry : std::filesystem::directory_iterator(absPathToFiles))
    {
        testFiles.push_back(entry.path());
    }

    return testFiles;
}
