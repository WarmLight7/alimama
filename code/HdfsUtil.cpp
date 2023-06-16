#include<HdfsUtil.hpp>
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#include <iomanip>

std::vector<std::string> HdfsUtil::splitString(const std::string& str, char delimiter) {
    // splitString 实现
    std::vector<std::string> result;
    std::istringstream iss(str);
    std::string token;
    while (std::getline(iss, token, delimiter)) {
        result.push_back(token);
    }
    return result;
}

std::string HdfsUtil::splitLastString(const std::string& str, char delimiter) {
    // splitLastString 实现
    std::istringstream iss(str);
    std::string token;
    while (std::getline(iss, token, delimiter)) {
    }
    return token;
}
bool HdfsUtil::cmp(std::string &a,std::string &b){
    char delimiter = '_';
    std::vector<std::string> substring_a = splitString(a, delimiter);
    std::vector<std::string> substring_b = splitString(b, delimiter);
    for(int i = 1 ; i < substring_a.size() ; i++){
        int numa = std::stoi(substring_a[i]);
        int numb = std::stoi(substring_b[i]);
        if(numa != numb){
            return numa < numb;
        }
    }
    return a<b;
}

bool HdfsUtil::directoryExists(const std::string& folderPath) {
    struct stat info;
    if (stat(folderPath.c_str(), &info) != 0)
        return false;
    return (info.st_mode & S_IFDIR) != 0;
}

bool HdfsUtil::createDirectory(const std::string& folderPath) {
    if (mkdir(folderPath.c_str(), 0777) == 0)
        return true;
    else
        return false;
}

std::string HdfsUtil::FindSlicePath(std::string modelPath, int slice_partition, int sliceLenth){
    std::stringstream ss;
    ss << std::setw(sliceLenth) << std::setfill('0') << slice_partition;
    std::string slicePartition = ss.str();
    std::string slicePath = modelPath + "model_slice." + ss.str();
    return slicePath;
}