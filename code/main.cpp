#include<bits/stdc++.h>
#include"ModelSliceReader.h"
#include <hdfs/hdfs.h>
using namespace std;

struct SliceRequests{
    uint64_t slice_partition = 1;
    uint64_t data_start = 2;
    uint64_t data_len = 3;
}SliceRequest;
std::vector<std::string> splitString(const std::string& str, char delimiter) {
    std::vector<std::string> result;
    std::istringstream iss(str);
    std::string token;
    while (std::getline(iss, token, delimiter)) {
        result.push_back(token);
    }
    return result;
}

bool cmp(string &a,string &b){
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
int main() {
    // HDFS连接配置
    std::string hdfsUrl = "hdfs://namenode:9000";
    std::string path = "/backup/";


    // 连接HDFS文件系统
    hdfsFS fs = hdfsConnect(hdfsUrl.c_str(), 0);
    if (!fs) {
        std::cerr << "Failed to connect to HDFS" << std::endl;
        return 1;
    }

    // 读取文件列表
    int numEntries = 0;
    hdfsFileInfo* fileInfo = hdfsListDirectory(fs, path.c_str(), &numEntries);
    if (!fileInfo) {
        std::cerr << "Failed to list directory: " << path << std::endl;
        hdfsDisconnect(fs);
        return -1;
    }
    std::priority_queue<string,vector<string>,decltype(&cmp)> fileNamePq(cmp);
    std::string targetFileName;
    bool rollbackFlag = false;
    for (int i = 0; i < numEntries; ++i) {
        std::string filePath = fileInfo[i].mName;
        std::string fileName;
        int fileType = fileInfo[i].mKind;
        size_t lastSlashPos = filePath.find_last_of('/');
        if (lastSlashPos != std::string::npos) {
            fileName = filePath.substr(lastSlashPos + 1);
        }
        if(fileType == kObjectKindDirectory && fileName.substr(0, 5).compare("model")==0){
            fileNamePq.push(fileName);
        }
        if(fileType == kObjectKindFile && fileName=="rollback.version"){
            rollbackFlag = true;
            break;
        }
    }
    cout << "fileNamePq.size:" << fileNamePq.size() << endl;
    hdfsFreeFileInfo(fileInfo, numEntries);

    // 确定目标文件名
    if(!rollbackFlag && ! fileNamePq.empty()){
        bool targetFileFlag = false;
        while(!targetFileFlag){
            targetFileName = fileNamePq.top();
            std::string hdfsPath = path + targetFileName + '/';
            numEntries = 0;
            fileInfo = hdfsListDirectory(fs, hdfsPath.c_str(), &numEntries);
            if (!fileInfo) {
                std::cerr << "Failed to list directory: " << path << std::endl;
                hdfsDisconnect(fs);
                return -1;
            }
            for (int i = 0; i < numEntries; ++i) {
                std::string filePath = fileInfo[i].mName;
                std::string fileName;
                size_t lastSlashPos = filePath.find_last_of('/');
                if (lastSlashPos != std::string::npos) {
                    fileName = filePath.substr(lastSlashPos + 1);
                }
                if(fileName == "model.done"){
                    targetFileFlag = true;
                    break;
                }
            }
            hdfsFreeFileInfo(fileInfo, numEntries);
            if(!targetFileFlag){
                fileNamePq.pop();
            }
        }
    }
    if(rollbackFlag){
        std::string hdfsPath = path + "rollback.version/";
        hdfsFile file = hdfsOpenFile(fs, hdfsPath.c_str(), O_RDONLY, 0, 0, 0);
        if (!file) {
            std::cerr << "无法打开文件" << std::endl;
            hdfsDisconnect(fs);
            return 1;
        }
        std::string content;
        const int checkBufferSize = 32;
        char readBuffer[checkBufferSize];
        int bytesRead = hdfsRead(fs, file, readBuffer, checkBufferSize);
        while (bytesRead > 0) {
            content.append(readBuffer, bytesRead);
            bytesRead = hdfsRead(fs, file, readBuffer, checkBufferSize);
        }
        hdfsCloseFile(fs, file);
        targetFileName = content;
    }
    if(targetFileName == ""){
        std::cerr << "Failed to find model: " << path << std::endl;
        hdfsDisconnect(fs);
        return -1;
    }
    hdfsDisconnect(fs);
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << SliceRequest.slice_partition;
    std::string slicePartiton = ss.str();
    std::string modelPath = path + targetFileName + "/model_slice." + slicePartiton + '/';
    cout << modelPath << endl;

    ModelSliceReader myModelSliceReader;
    myModelSliceReader.Load(modelPath);
    char *data_buffer;
    myModelSliceReader.Read(SliceRequest.data_start, SliceRequest.data_len, data_buffer);
    myModelSliceReader.Unload();
    cout << data_buffer << endl;






}
// g++ -o test test.cpp -lhdfs -usr/lacal/include/hdfs -lmodel_slice_reader
