
#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <queue>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <cstdlib>
#include <ctime>
#include <sys/stat.h>
#include <unordered_set>
#include <thread>

#include <etcd/Client.hpp>
#include <etcd/Response.hpp>
#include <pplx/pplxtasks.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#include <ifaddrs.h>
#include <netinet/in.h>  // IPv4 结构体定义和相关函数
#include <arpa/inet.h>  // 网络地址转换函数

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

#include"ModelSliceReader.h"
#include <hdfs/hdfs.h>


using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using helloworld::SliceRequest;
using helloworld::Request;
using helloworld::Response;
using helloworld::Greeter;



using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;



class ModelPathFinder{
private:
    std::string hdfsUrl;
    std::string path;
    hdfsFS fs;
    int sliceLenth;

public:
    ModelPathFinder() {
        hdfsUrl = "hdfs://namenode:9000";
        path = "/";
        fs = hdfsConnect(this->hdfsUrl.c_str(), 0);
        if (!fs) {
            std::cerr << "Failed to connect to HDFS" << std::endl;
        }
        sliceLenth = 0;
    }
    ModelPathFinder(const std::string& hdfsUrl, const std::string& path) : hdfsUrl(hdfsUrl), path(path) {
        fs = hdfsConnect(this->hdfsUrl.c_str(), 0);
        if (!fs) {
            std::cerr << "Failed to connect to HDFS" << std::endl;
        }
        sliceLenth = 0;
    }
    ModelPathFinder(const ModelPathFinder& modelPathFinder){
        this->hdfsUrl = modelPathFinder.hdfsUrl;
        this->path = modelPathFinder.path;
        this->fs = hdfsConnect(this->hdfsUrl.c_str(), 0);
        if (!this->fs) {
            std::cerr << "Failed to connect to HDFS" << std::endl;
        }
        this->sliceLenth = modelPathFinder.sliceLenth;
    }
    std::vector<std::string> splitString(const std::string& str, char delimiter) {
        std::vector<std::string> result;
        std::istringstream iss(str);
        std::string token;
        while (std::getline(iss, token, delimiter)) {
            result.push_back(token);
        }
        return result;
    }
    std::string splitLastString(const std::string& str, char delimiter) {
        std::istringstream iss(str);
        std::string token;
        while (std::getline(iss, token, delimiter)) {
        }
        return token;
    }

    bool cmp(std::string &a,std::string &b){
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
    std::string FindSlicePath(std::string modelPath, int slice_partition){
        std::stringstream ss;
        ss << std::setw(sliceLenth) << std::setfill('0') << slice_partition;
        std::string slicePartition = ss.str();
        std::string slicePath = modelPath + "model_slice." + ss.str();
        return slicePath;
    }
    bool directoryExists(const std::string& folderPath) {
        struct stat info;
        if (stat(folderPath.c_str(), &info) != 0)
            return false;
        return (info.st_mode & S_IFDIR) != 0;
    }

    bool createDirectory(const std::string& folderPath) {
        if (mkdir(folderPath.c_str(), 0777) == 0)
            return true;
        else
            return false;
    }
    void moveToDisk(std::string modelPath, std::string slicePath, std::string localSlicePath){
        std::cout << modelPath << std::endl;
        std::cout << slicePath << std::endl;
        std::string localModelPath = "/work"+modelPath;
        std::cout << localModelPath << std::endl;
        std::cout << localSlicePath << std::endl;
        if(!directoryExists(localModelPath)){
            createDirectory(localModelPath);
        }
        std::cout << "创建文件夹成功" << std::endl;

        hdfsFile file = hdfsOpenFile(fs, slicePath.c_str(), O_RDONLY, 0, 0, 0);
        std::cout << "hdfs链接成功" << std::endl;

        FILE* localFile = fopen(localSlicePath.c_str(), "wb");
        std::cout << "本地文件打开成功" << std::endl;
        char buffer[4096];
        int bytesRead = hdfsRead(fs, file, buffer, sizeof(buffer));
        std::cout << "开始读取" << std::endl;
        while (bytesRead > 0) {
            fwrite(buffer, 1, bytesRead, localFile);
            bytesRead = hdfsRead(fs, file, buffer, sizeof(buffer));
        }
        std::cout << "引动到磁盘成功" << std::endl;
        hdfsCloseFile(fs, file);
        fclose(localFile);
    }
    std::string FindModelPath() {
        int numEntries = 0;
        hdfsFileInfo* fileInfo = hdfsListDirectory(fs, path.c_str(), &numEntries);
        if (!fileInfo) {
            std::cerr << "Failed to list directory: " << path << std::endl;
        }
        auto cmpFunc = [this](std::string& a, std::string& b) {
            return cmp(a, b);
        };
        std::priority_queue<std::string, std::vector<std::string>, std::function<bool(std::string&, std::string&)>> fileNamePq(cmpFunc);
        std::string targetFileName;
        bool rollbackFlag = false;
        for (int i = 0; i < numEntries; ++i) {
            std::string filePath = fileInfo[i].mName;
            std::string fileName;
            int fileType = fileInfo[i].mKind;
            std::cout << fileName << fileType << std::endl;
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
        std::cout << "fileNamePq.size:" << fileNamePq.size() << std::endl;
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
                }
                for (int i = 0; i < numEntries; ++i) {
                    std::string filePath = fileInfo[i].mName;
                    std::string fileName;
                    size_t lastSlashPos = filePath.find_last_of('/');
                    if (lastSlashPos != std::string::npos) {
                        fileName = filePath.substr(lastSlashPos + 1);
                    }
                    std::vector<std::string> splitedFileName = splitString(fileName, '.');
                    if(splitedFileName[0] == "model_slice"){
                        sliceLenth = splitedFileName[1].size();
                    }
                    if(fileName == "model.done"){
                        targetFileFlag = true;
                    }

                }
                hdfsFreeFileInfo(fileInfo, numEntries);
                if(!targetFileFlag){
                    fileNamePq.pop();
                }
            }
        }
        if(rollbackFlag){
            std::string rollbackHdfsPath = path + "rollback.version/";
            hdfsFile file = hdfsOpenFile(fs, rollbackHdfsPath.c_str(), O_RDONLY, 0, 0, 0);
            if (!file) {
                std::cerr << "无法打开文件" << std::endl;
            }
            std::string content;
            const int checkBufferSize = 1024;
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
        }

        std::string modelPath = path + targetFileName + '/';
        return modelPath;
    }
    void disconnect(){
        hdfsDisconnect(fs);
    }

};

int port = 50051;
const int sliceReaderNumber = 41;

// grpc客户端
class GreeterClient {

public:
    GreeterClient(std::shared_ptr<Channel> channel)
            : stub_(Greeter::NewStub(channel)) {}

    // Assembles the client's payload, sends it and presents the response back
    // from the server.
    std::string Get(int slice_partition, int data_start, int data_len) {

        helloworld::Request request;
        // 一次请求一个切片（主机间通信）
        helloworld::SliceRequest* slice = request.add_slice_request();
        slice->set_slice_partition(slice_partition);
        slice->set_data_start(data_start);
        slice->set_data_len(data_len);

        helloworld::Response reply;
        ClientContext context;
        Status status = stub_->Get(&context, request, &reply);

        if (status.ok()) {
            std::string res = "";
            for (const std::string& slice : reply.slice_data()) {
                res += slice;
            }
            return res;
        } else {
            std::cout << status.error_code() << ": " << status.error_message()
                      << std::endl;
            return "RPC failed";
        }
    }

private:
    std::unique_ptr<Greeter::Stub> stub_;
};


// etcd注册地址
std::string getLocalIP() {
    struct ifaddrs *ifAddrStruct = NULL;
    void *tmpAddrPtr = NULL;
    std::string localIP;
    getifaddrs(&ifAddrStruct);
    while (ifAddrStruct != NULL) {
        if (ifAddrStruct->ifa_addr->sa_family == AF_INET) {
            tmpAddrPtr = &((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
            std::string interfaceName(ifAddrStruct->ifa_name);
            if (interfaceName == "en0" || interfaceName == "eth0") {
                return addressBuffer;
            }
        }
        ifAddrStruct = ifAddrStruct->ifa_next;
    }
    return "";
}

// etcd写
void writeData(etcd::Client& etcd, const std::string& key, const std::string& value) {
    etcd::Response response = etcd.set(key, value).get();

    if (response.is_ok()) {
        std::cout << "etcd写入成功" << std::endl;
    } else {
        std::cerr << "etcd写入失败: " << response.error_message() << std::endl;
    }
}

// etcd读
std::string readData(etcd::Client& etcd, const std::string& key) {
    pplx::task<etcd::Response> responseTask = etcd.get(key);
    responseTask.wait(); // 等待任务完成

    etcd::Response response = responseTask.get();

    if (response.is_ok()) {
        return response.value().as_string();
    } else {
        return "";
    }
    return "";
}

// etcd注册本主机
void regist(etcd::Client& myetcd)
{
    std::string external_address = getLocalIP() + std::string(":") + std::to_string(port);

    std::string key = std::string("/services/modelservice/") +  external_address;
    writeData(myetcd, key, external_address);
    // 维护当前版本信息
    writeData(myetcd,"nowModelVersion","");
}


// grpc服务定义
class GreeterServiceImpl final : public Greeter::Service {
private:
    etcd::Client& myetcd;
    ModelSliceReader sliceReaderList[sliceReaderNumber];
    std::string localIPPort;
    std::string sliceReaderKeyList[sliceReaderNumber];
    std::string hdfsUrl = "hdfs://namenode:9000";
    std::string path = "/";
    ModelPathFinder modelPathFinder;
public:
    GreeterServiceImpl(etcd::Client& etcdClient) : myetcd(etcdClient) {
        localIPPort = getLocalIP() + std::string(":") + std::to_string(port);
        for (int sliceCount = 0; sliceCount < sliceReaderNumber; sliceCount++) {
            sliceReaderKeyList[sliceCount] = "";
        }
        modelPathFinder = ModelPathFinder(hdfsUrl, path);
    }

    // 多线程HDFS读
    void ReadFromHDFSThread(std::string modelPath, std::string slicePath, std::string localSlicePath)
    {
        modelPathFinder.moveToDisk(modelPath, slicePath, localSlicePath);
        writeData(myetcd, slicePath, localIPPort);
        ReadFromDisk(localSlicePath);
    }

    // 多线程磁盘读
    void ReadFromDiskThread(std::string localSlicePath)
    {
        ReadFromDisk(localSlicePath);
    }

    void ReadFromDisk(std::string& localSlicePath){
        std::cout << "即将从本机磁盘读取..."<< std::endl;
        int sliceCount = -1;
        for (int valuableSliceCount = 0; valuableSliceCount < sliceReaderNumber; valuableSliceCount++){
            if(sliceReaderKeyList[valuableSliceCount]==""){
                sliceCount = valuableSliceCount;
                std::cout << "获取到空闲sliceReader节点:"<< sliceCount <<std::endl;
                break;
            }
        }
        if(sliceCount == -1){
            std::srand(std::time(nullptr)); // 设置随机种子
            int sliceCount = std::rand() % sliceReaderNumber;
            std::cout << "sliceReader已满卸载原有节点:"<< sliceCount <<std::endl;
            sliceReaderList[sliceCount].Unload();
            writeData(myetcd, sliceReaderKeyList[sliceCount], "");
            sliceReaderKeyList[sliceCount]="";
        }
        std::cout << "装载当前节点:"<< sliceCount <<std::endl;
        sliceReaderList[sliceCount].Load(localSlicePath);
        sliceReaderKeyList[sliceCount]=localSlicePath;
        writeData(myetcd, localSlicePath, localIPPort+"_"+std::to_string(sliceCount));
        // std::cout << "从当前节点读取数据:"<< sliceCount <<std::endl;
        // sliceReaderList[sliceCount].Read(req.data_start(),req.data_len(), data_buffer);
    }

    Status Get(ServerContext* context, const Request* request,
               Response* response) override {
        
        // 1.获取模型版本信息
        response->set_status(0);
        
        std::string modelPath = modelPathFinder.FindModelPath();
        std::string nowModelVersion = readData(myetcd, "nowModelVersion");
        // key-> value: slicePath->localIP "nowModelVersion"->modelPath
        // localSlicePath->(localIP)_(sliceCount) sliceReaderKeyList->localSlicePath

        if(modelPath != nowModelVersion){
            // 清空当前节点所缓存的切片
            std::cout << "清空当前节点内存中的切片"<< std::endl;
            for (int sliceCount = 0; sliceCount < sliceReaderNumber; sliceCount++){
                if(sliceReaderKeyList[sliceCount]!=""){
                    sliceReaderList[sliceCount].Unload();
                    writeData(myetcd, sliceReaderKeyList[sliceCount], "");
                    sliceReaderKeyList[sliceCount]="";
                }
            }
            //更新当前模型版本
            nowModelVersion = modelPath;
            writeData(myetcd,"nowModelVersion", nowModelVersion);
        }

        // 2.加载本机需要的切片到内存（多线程）
        
        // 切片分组
        std::unordered_set<int> slicePartitionSet;
        for(auto &req : request->slice_request()){
            slicePartitionSet.insert(req.slice_partition());
        }
        // 多线程在这里开始工作
        // 2.1 检查需要本机加载的分组
        for(auto& slicePartition : slicePartitionSet){
            
            std::string slicePath = modelPathFinder.FindSlicePath(modelPath, slicePartition);
            std::string localSlicePath = "/work"+slicePath;
            std::string diskCache = readData(myetcd, slicePath);
            std::string memoryCache = readData(myetcd, localSlicePath);

            // 2.1.1 首先区分磁盘缓存是否为空
            if(diskCache == ""){
                std::cout << "没有磁盘缓存该切片，即将从hadoop读取..."<< std::endl;

                std::thread modelPathFinderThread(&GreeterServiceImpl::ReadFromHDFSThread, this, modelPath, slicePath, localSlicePath ); // 创建线程，并绑定到类的成员函数，并传递参数
                modelPathFinderThread.detach(); // 分离线程，使其成为一个独立的后台线程

                std::thread readFromDiskThread(&GreeterServiceImpl::ReadFromDiskThread, this, localSlicePath); // 创建线程，并绑定到类的成员函数，并传递参数
                readFromDiskThread.detach(); // 分离线程，使其成为一个独立的后台线程
            }

            // 2.1.2 其次区分磁盘缓存是否为本机
            else if(diskCache == localIPPort && memoryCache == "")
            {
                std::cout << "本机内存没有该切片，即将从本机磁盘读取..."<< std::endl;
                std::thread readFromDiskThread(&GreeterServiceImpl::ReadFromDiskThread, this, localSlicePath); // 创建线程，并绑定到类的成员函数，并传递参数
                readFromDiskThread.detach(); // 分离线程，使其成为一个独立的后台线程
            }
            else if(diskCache == localIPPort && memoryCache != "") // 这种情况表示已经在内存了
            {
                std::cout << "数据已位于本机内存..."<< std::endl;
            }
            else if(diskCache != localIPPort) // 版本回滚
            {
                std::cout << "从"<< diskCache <<"读取（先让目标主机把数据加载到内存）..."<< std::endl;
                auto req = sliceRequestList[slicePartition][0];
                GreeterClient greeter(grpc::CreateChannel(diskCache, grpc::InsecureChannelCredentials()));
                // data_buffer = const_cast<char*>(greeter.Get(req.slice_partition(),req.data_start(),req.data_len()).c_str());
                greeter.Get(req.slice_partition(),req.data_start(),req.data_len()).c_str();
            }
            else
            {
                std::cout << "出错，未考虑到相关情况，检查代码！"<< std::endl;
            }
        }

        for(auto &req : request->slice_request()){
            std::string slicePath = modelPathFinder.FindSlicePath(modelPath, req.slice_partition());
            std::string localSlicePath = "/work"+slicePath;
            std::string diskCache = readData(myetcd, slicePath);
            std::string memoryCache = readData(myetcd, localSlicePath);
            char data_buffer[req.data_len()];
            while(memoryCache == "")
            {
                std::cout << "多线程正在读取，请稍后..."<< std::endl;
                memoryCache = readData(myetcd, localSlicePath);
            }
            if(memoryCache != localIPPort+"_"+std::to_string(req.slice_partition())){
                std::cout << "从"<< diskCache <<"读取..."<< std::endl;
                GreeterClient greeter(grpc::CreateChannel(
                    diskCache, grpc::InsecureChannelCredentials()));
                // data_buffer = const_cast<char*>(greeter.Get(req.slice_partition(),req.data_start(),req.data_len()).c_str());
                std::string get_buffer = greeter.Get(req.slice_partition(),req.data_start(),req.data_len()).c_str();
                strcpy(data_buffer, get_buffer.c_str());
            }
            else{
                std::cout << "即将从本机内存直接加载..."<< std::endl;
                int sliceCount = std::stoi(modelPathFinder.splitLastString(memoryCache, '_'));
                std::cout << "sliceReader节点:"<< sliceCount << std::endl;
                sliceReaderList[sliceCount].Read(req.data_start(),req.data_len(), data_buffer);
                    //etcd标记该切片
            }
            response->add_slice_data(data_buffer);
            std::cout << "切片添加成功！" << std::endl;
        }
        modelPathFinder.disconnect();



        return Status::OK;
    }
};

//grpc服务运行
void RunServer(etcd::Client& myetcd) {

    std::string server_address("0.0.0.0:50051");

    GreeterServiceImpl service(myetcd);
    regist(myetcd);
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}




int main(int argc, char** argv) {
    // etcd连接
    etcd::Client myetcd("http://etcd:2379");

    RunServer(myetcd);

    /*以下为客户端代码，供测试使用*/
    /*
    int slicenum, start, len;
    std::string diskCache;
    while(1)
    {
        std::cout << "输入切片编号、开始位置、读取长度："<< std::endl;
        std::cin>>slicenum>>start>>len;
        std::cout << "输入grpc地址（IP:50051）（可以是本机）："<< std::endl;
        std::cin>>diskCache;
        char* data_buffer;
        std::cout << "正在读取..."<< std::endl;
        GreeterClient greeter(grpc::CreateChannel(diskCache, grpc::InsecureChannelCredentials()));
        data_buffer = const_cast<char*>(greeter.Get(slicenum,start,len).c_str());
        std::cout << "获得内容："<< std::endl;
        std::cout<<std::string(data_buffer)<<std::endl;
    }

     */


    return 0;
}
