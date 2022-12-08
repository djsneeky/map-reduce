#ifndef FILEHELPER_H
#define FILEHELPER_H

#include <string>
#include <vector>

std::string getExecutablePath();
std::vector<std::string> getListOfTestFiles(std::string dirPath, int repeatFile);

#endif //FILEHELPER_H
