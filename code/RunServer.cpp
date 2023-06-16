#include "EtcdUtil.hpp"
#include "greeter_server.hpp"
#include "HdfsReader.hpp"
#include "HdfsUtil.hpp"






//grpc服务运行

void RunServer() {
    // etcd连接

    EtcdUtil::EtcdUtil myetcd;

    HdfsReader HdfsReaderClients[4];

    std::string server_address("0.0.0.0:50051");
    
    GreeterServiceImpl service(myetcd, HdfsReaderClients);

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
    
    
    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    
    // 写入etcd本节点加载到内存完成
    writeData(myetcd, "/readyToRegist/node-" + std::string(std::getenv("NODE_ID")), "readyToRegist");



    
    if(test){
        std::string node1, node3;
        while(node1 == "" || node3 == "")
        {
            if(node1 == "") node1 = readData(myetcd,"/readyToRegist/node-1");
            if(node3 == "") node3 = readData(myetcd,"/readyToRegist/node-3");
        }
    }
    else{
        std::string node1, node2, node3, node4, node5, node6;
        while(node1 == "" || node2 == "" || node3 == "" || node4 == "" || node5 == "" || node6 == "")
        {
            if(node1 == "") node1 = readData(myetcd,"/readyToRegist/node-1");
            if(node2 == "") node2 = readData(myetcd,"/readyToRegist/node-2");
            if(node3 == "") node3 = readData(myetcd,"/readyToRegist/node-3");
            if(node4 == "") node4 = readData(myetcd,"/readyToRegist/node-4");
            if(node5 == "") node5 = readData(myetcd,"/readyToRegist/node-5");
            if(node6 == "") node6 = readData(myetcd,"/readyToRegist/node-6");
        }
    }

    
    std::cout << "Server listening on " << server_address << std::endl;
    regist(myetcd);
    if(test){
        myetcd.rm("/readyToRegist/node-" + std::string(std::getenv("NODE_ID")));
    }
    
    server->Wait();
}



int main(int argc, char** argv) {

    RunServer();

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
// 
    return 0;
}
