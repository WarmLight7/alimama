#include <string>
#include <set>
#include <vector>
#include <hdfs/hdfs.h>

class HdfsReader{
private:
    std::string hdfsUrl="hdfs://namenode:9000";
    std::string path="/";
    hdfsFS fs;
    int sliceLenth=3;
    int sliceNumber=240;
    bool backupReady = false;
    std::set<std::string> modelSet;

public:
    HdfsReader() {}

    HdfsReader(const HdfsReader& HdfsReader){}
    
    bool findPathFlag(std::string modelPath){return modelSet.find(modelPath) != modelSet.end();}
    
    int getSliceNumber();

    std::vector<std::string> getModelList();

    void moveToDisk(std::string modelPath, std::string slicePath, std::string localSlicePath, int hdfsNumber);

    std::string FindModelPath(bool coutFlag);
    
    void disconnect(){hdfsDisconnect(fs);}

};

