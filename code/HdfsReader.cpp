#include "HdfsReader.hpp"
#include <iostream>
#include "HdfsUtil.hpp"

char readFileBuffer[4][33554432];

HdfsReader::HdfsReader()
{
    if (test)
    {
        path = "/backup/";
        if (!HdfsUtil::directoryExists("/work/backup/"))
        {
            HdfsUtil::createDirectory("/work/backup/");
        }
    }
    fs = hdfsConnect(this->hdfsUrl.c_str(), 0);
    std::cout << "HdfsReader注册成功" << std::endl;
}

HdfsReader::HdfsReader(const HdfsReader &HdfsReader)
{
    this->hdfsUrl = HdfsReader.hdfsUrl;
    this->path = HdfsReader.path;
    fs = hdfsConnect(this->hdfsUrl.c_str(), 0);
    this->sliceLenth = HdfsReader.sliceLenth;
    std::cout << "HdfsReader赋值成功" << std::endl;
}

bool HdfsReader::findPathFlag(std::string modelPath)
{
    return modelSet.find(modelPath) != modelSet.end();
}

int HdfsReader::getSliceNumber()
{
    if (sliceNumber < 128)
    {
        return 20;
    }
    if (sliceNumber < 240)
    {
        return 128;
    }
    return 240;
}
std::vector<std::string> HdfsReader::getModelList()
{
    int numEntries = 0;
    hdfsFileInfo *fileInfo = hdfsListDirectory(fs, path.c_str(), &numEntries);
    std::vector<std::string> modelList;

    for (int i = 0; i < numEntries; ++i)
    {
        std::string filePath = fileInfo[i].mName;
        std::string fileName;
        int fileType = fileInfo[i].mKind;
        size_t lastSlashPos = filePath.find_last_of('/');
        if (lastSlashPos != std::string::npos)
        {
            fileName = filePath.substr(lastSlashPos + 1);
        }
        if (fileType == kObjectKindDirectory && fileName.substr(0, 5).compare("model") == 0)
        {
            modelList.emplace_back(path + fileName + '/');
        }
    }
    return modelList;
}

void HdfsReader::moveToDisk(std::string modelPath, std::string slicePath, std::string localSlicePath, int hdfsNumber)
{
    std::string localModelPath = "/work" + modelPath;
    std::cout << localModelPath << std::endl;
    std::cout << localSlicePath << std::endl;
    if (!HdfsUtil::directoryExists(localModelPath))
    {
        HdfsUtil::createDirectory(localModelPath);
        std::cout << "创建文件夹成功" << std::endl;
    }
    hdfsFile file = hdfsOpenFile(fs, slicePath.c_str(), O_RDONLY, 0, 0, 0);
    std::cout << "hdfs链接成功" << std::endl;
    FILE *localFile = fopen(localSlicePath.c_str(), "wb");
    std::cout << "本地文件打开成功" << std::endl;
    int bytesRead = hdfsRead(fs, file, readFileBuffer[hdfsNumber], sizeof(readFileBuffer[hdfsNumber]));
    std::cout << "开始读取" << std::endl;
    while (bytesRead > 0)
    {
        fwrite(readFileBuffer[hdfsNumber], 1, bytesRead, localFile);
        bytesRead = hdfsRead(fs, file, readFileBuffer[hdfsNumber], sizeof(readFileBuffer[hdfsNumber]));
    }
    std::cout << "引动到磁盘成功" << std::endl;
    hdfsCloseFile(fs, file);
    fclose(localFile);
    backupReady = true;
}

std::string HdfsReader::FindModelPath(bool coutFlag = true)
{
    std::string targetFileName = "";
    while (targetFileName == "")
    {
        int numEntries = 0;
        hdfsFileInfo *fileInfo = hdfsListDirectory(fs, path.c_str(), &numEntries);
        auto cmpFunc = [this](std::string &a, std::string &b)
        {
            return HdfsUtil::cmp(a, b);
        };
        std::priority_queue<std::string, std::vector<std::string>, std::function<bool(std::string &, std::string &)>> fileNamePq(cmpFunc);
        bool rollbackFlag = false;
        for (int i = 0; i < numEntries; ++i)
        {
            std::string filePath = fileInfo[i].mName;
            std::string fileName;
            int fileType = fileInfo[i].mKind;
            size_t lastSlashPos = filePath.find_last_of('/');
            if (lastSlashPos != std::string::npos)
            {
                fileName = filePath.substr(lastSlashPos + 1);
            }
            if (coutFlag)
            {
                std::cout << fileName << fileType << std::endl;
            }

            if (fileType == kObjectKindDirectory && fileName.substr(0, 5).compare("model") == 0)
            {
                fileNamePq.push(fileName);
            }
            if (fileType == kObjectKindFile && fileName == "rollback.version")
            {
                rollbackFlag = true;
                break;
            }
        }
        if (coutFlag)
        {
            std::cout << "fileNamePq.size:" << fileNamePq.size() << std::endl;
        }
        hdfsFreeFileInfo(fileInfo, numEntries);
        // 确定目标文件名
        if (!rollbackFlag && !fileNamePq.empty())
        {
            bool targetFileFlag = false;
            while (!targetFileFlag)
            {
                targetFileName = fileNamePq.top();
                std::string hdfsPath = path + targetFileName + '/';
                numEntries = 0;
                // std::cout << "hdfsPath" << hdfsPath << std::endl;
                fileInfo = hdfsListDirectory(fs, hdfsPath.c_str(), &numEntries);
                if (!fileInfo)
                {
                    std::cerr << "Failed to list directory: " << path << std::endl;
                }
                for (int i = 0; i < numEntries; ++i)
                {
                    std::string filePath = fileInfo[i].mName;
                    std::string fileName;
                    size_t lastSlashPos = filePath.find_last_of('/');
                    if (lastSlashPos != std::string::npos)
                    {
                        fileName = filePath.substr(lastSlashPos + 1);
                    }
                    // std::cout << "fileName" << fileName << std::endl;
                    std::vector<std::string> splitedFileName = HdfsUtil::splitString(fileName, '.');
                    if (splitedFileName[0] == "model_slice")
                    {
                        sliceLenth = splitedFileName[1].size();
                    }
                    if (fileName == "model.done")
                    {
                        sliceNumber = numEntries;
                        targetFileFlag = true;
                    }
                }
                hdfsFreeFileInfo(fileInfo, numEntries);
                if (!targetFileFlag)
                {
                    fileNamePq.pop();
                }
            }
        }
        if (rollbackFlag)
        {
            std::string rollbackHdfsPath = path + "rollback.version";
            hdfsFile file = hdfsOpenFile(fs, rollbackHdfsPath.c_str(), O_RDONLY, 0, 0, 0);
            std::string content;
            const int checkBufferSize = 1024;
            char readBuffer[checkBufferSize];
            int bytesRead = hdfsRead(fs, file, readBuffer, checkBufferSize);
            while (bytesRead > 0)
            {
                content.append(readBuffer, bytesRead);
                bytesRead = hdfsRead(fs, file, readBuffer, checkBufferSize);
            }
            hdfsCloseFile(fs, file);
            targetFileName = content.substr(0, content.size() - 1);
            bool modelDone = false;
            while (!modelDone)
            {
                std::string hdfsPath = path + targetFileName + '/';
                numEntries = 0;
                // std::cout << "hdfsPath" << hdfsPath << std::endl;
                fileInfo = hdfsListDirectory(fs, hdfsPath.c_str(), &numEntries);
                for (int i = 0; i < numEntries; ++i)
                {
                    std::string filePath = fileInfo[i].mName;
                    std::string fileName;
                    size_t lastSlashPos = filePath.find_last_of('/');
                    if (lastSlashPos != std::string::npos)
                    {
                        fileName = filePath.substr(lastSlashPos + 1);
                    }
                    if (fileName == "model.done")
                    {
                        sliceNumber = numEntries;
                        modelDone = true;
                    }
                }
                hdfsFreeFileInfo(fileInfo, numEntries);
            }
        }
    }
    std::string modelPath = path + targetFileName + '/';
    if (coutFlag)
    {
        std::cout << "targetFileName: " << targetFileName << std::endl;
        std::cout << "modelPath: " << modelPath << std::endl;
    }

    return modelPath;
}