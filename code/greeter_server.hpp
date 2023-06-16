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

#include "EtcdUtil.hpp"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using alimama::proto::SliceRequest;
using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::ModelService;


using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;


const bool test = false;
const bool getCout = false;
const bool readVersion = true;

class GreeterServiceImpl final : public ModelService::Service {
private:
    EtcdUtil::EtcdUtil& myetcd;
    static const int sliceReaderNumber = 121;
    ModelSliceReader sliceReaderList[sliceReaderNumber];
    std::string localIPPort;
    std::string sliceReaderKeyList[sliceReaderNumber];
    HdfsReader HdfsReader[4];
    std::string nowModelVersion;
    int hostNode;
    int sliceNumber;
    int requestVersion;
    GreeterClient greeter[7];
    // 请求队列，瞬时请求大于这个qps的请求将被舍弃
    int maxqps = 100;
    std::atomic<int> curCount; // 当前请求量

    // 空耗线程停止控制变量
    bool switchVersion = true;

    // 实现消息队列/生产者消费者模型的三个变量
    std::queue<alimama::proto::Request*> waitQueue;
    std::mutex mtx;
    std::condition_variable cv;
    
public:
    GreeterServiceImpl(EtcdUtil::EtcdUtil& etcdClient, HdfsReader* HdfsReaderClients);

    void versionChangeAction(std::string modelPath, int hdfsNumber, bool moveFlag);

    void versionChange(std::string modelPath, bool moveFlag);

    void modelVersionThreadFunction();

    void limitQPS();
    
    void ReadFromDisk(int sliceReaderCount, std::string localSlicePath);

    Status Get(ServerContext* context, const Request* request, Response* response);
};

