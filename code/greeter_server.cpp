#include "greeter_server.hpp"

GreeterServiceImpl::GreeterServiceImpl(EtcdUtil::EtcdUtil& etcdClient, HdfsReader *HdfsReaderClients) : myetcd(etcdClient)
{
    for (int i = 0; i < 4; i++)
    {
        HdfsReader[i] = HdfsReaderClients[i];
    }
    if (test)
    {
        for (int i = 1; i <= 3; i += 2)
        {
            if (i == hostNode)
            {
                continue;
            }
            std::string distNode = "node-" + std::to_string(i) + ":50051";
            greeter[i] = GreeterClient(grpc::CreateChannel(distNode, grpc::InsecureChannelCredentials()));
        }
    }
    else
    {
        for (int i = 1; i <= 6; i++)
        {
            if (i == hostNode)
            {
                continue;
            }
            std::string distNode = "node-" + std::to_string(i) + ":50051";
            greeter[i] = GreeterClient(grpc::CreateChannel(distNode, grpc::InsecureChannelCredentials()));
        }
    }

    std::string modelPath = HdfsReader[0].FindModelPath(false);
    nowModelVersion = modelPath;
    requestVersion = 0;
    sliceNumber = HdfsReader[0].getSliceNumber();
    localIPPort = getLocalIP() + std::string(":") + std::to_string(port);
    for (int sliceCount = 0; sliceCount < sliceReaderNumber; sliceCount++)
    {
        sliceReaderKeyList[sliceCount] = "";
    }

    char *hostname = std::getenv("NODE_ID");
    hostNode = std::stoi(hostname);
    int slicePartition = 0;
    std::cout << " hostNode:" << hostNode << std::endl;
    for (int sliceReaderCount = 0; sliceReaderCount < sliceReaderNumber; sliceReaderCount++)
    {
        slicePartition = sliceReaderCount * 6 + hostNode - 1;
        if (slicePartition >= sliceNumber)
        {
            break;
        }
        std::string slicePath = HdfsReader[0].FindSlicePath(modelPath, slicePartition);
        std::string localSlicePath = "/work" + slicePath;
        HdfsReader[0].moveToDisk(modelPath, slicePath, localSlicePath, false, 1);
        ReadFromDisk(sliceReaderCount + requestVersion, localSlicePath);
    }
    std::vector<std::string> modelList = HdfsReader[0].getModelList();
    std::cout << "开始装载回滚版本" << std::endl;
    // 装载rollback版本
    for (auto &otherModelPath : modelList)
    {
        if (otherModelPath == modelPath)
        {
            continue;
        }
        slicePartition = 0;
        for (int sliceReaderCount = 0; sliceReaderCount < sliceReaderNumber; sliceReaderCount++)
        {
            slicePartition = sliceReaderCount * 6 + hostNode - 1;
            if (slicePartition >= sliceNumber)
            {
                break;
            }
            std::string slicePath = HdfsReader[0].FindSlicePath(otherModelPath, slicePartition);
            std::string localSlicePath = "/work" + slicePath;
            HdfsReader[0].moveToDisk(otherModelPath, slicePath, localSlicePath, false, 1);
            ReadFromDisk(sliceReaderCount + 80, localSlicePath);
        }
    }

    // 创建监听模型版本的线程
    std::thread modelVersionThread(&GreeterServiceImpl::modelVersionThreadFunction, this);
    modelVersionThread.detach();
    // 限制qps线程
    curCount = 0;
    std::thread limitQPS(&GreeterServiceImpl::limitQPS, this);
    limitQPS.detach();
}

void GreeterServiceImpl::versionChangeAction(std::string modelPath, int hdfsNumber, bool moveFlag)
{
    int start = (hdfsNumber - 1) * 10;
    int end = hdfsNumber * 10;

    int futureRequestVersion;

    if (readVersion)
    {
        futureRequestVersion = requestVersion + 40;
    }
    else
    {
        if (requestVersion == 0)
        {
            futureRequestVersion = 40;
        }
        else
        {
            futureRequestVersion = 0;
        }
    }

    int slicePartition = 0;
    std::cout << "moveFlag" << moveFlag << std::endl;
    for (int sliceReaderCount = start; sliceReaderCount < end; sliceReaderCount++)
    {
        slicePartition = sliceReaderCount * 6 + hostNode - 1;
        if (slicePartition >= sliceNumber)
        {
            break;
        }
        std::string slicePath = HdfsReader[0].FindSlicePath(modelPath, slicePartition);
        std::string localSlicePath = "/work" + slicePath;
        if (moveFlag)
        {
            HdfsReader[hdfsNumber].moveToDisk(modelPath, slicePath, localSlicePath, true, hdfsNumber);
        }
        ReadFromDisk(sliceReaderCount + futureRequestVersion, localSlicePath);
    }
}

void GreeterServiceImpl::versionChange(std::string modelPath, bool moveFlag)
{
    // 多线程加载
    std::thread versionChangeThreads[4];
    for (int i = 0; i < 4; i++)
    {
        versionChangeThreads[i] = std::thread(&GreeterServiceImpl::versionChangeAction, this, modelPath, i, moveFlag);
    }
    for (int i = 0; i < 4; i++)
    {
        versionChangeThreads[i].join();
    }

    if (test)
    {
        myetcd.writeData("/versionChange/node-" + std::string(std::getenv("NODE_ID")), modelPath);
        std::string node1 = "", node3 = "";
        while (node1 != modelPath || node3 != modelPath)
        {
            if (node1 != modelPath)
                node1 = myetcd.readData("/versionChange/node-1");
            if (node3 != modelPath)
                node3 = myetcd.readData("/versionChange/node-3");
        }
    }
    else
    {
        myetcd.writeData("/versionChange/node-" + std::string(std::getenv("NODE_ID")), modelPath);
        std::string node1 = "", node2 = "", node3 = "", node4 = "", node5 = "", node6 = "";
        while (node1 != modelPath || node2 != modelPath || node3 != modelPath || node4 != modelPath || node5 != modelPath || node6 != modelPath)
        {
            if (node1 != modelPath)
                node1 = myetcd.readData("/versionChange/node-1");
            if (node2 != modelPath)
                node2 = myetcd.readData("/versionChange/node-2");
            if (node3 != modelPath)
                node3 = myetcd.readData("/versionChange/node-3");
            if (node4 != modelPath)
                node4 = myetcd.readData("/versionChange/node-4");
            if (node5 != modelPath)
                node5 = myetcd.readData("/versionChange/node-5");
            if (node6 != modelPath)
                node6 = myetcd.readData("/versionChange/node-6");
            // std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    // 检测到版本切换时先把key:/versionChange/删了

    std::cout << "/versionChange/node-" + std::string(std::getenv("NODE_ID")) + "newVersionReadyToLoad" << std::endl;
    if (readVersion)
    {
        requestVersion += 40;
    }
    else
    {
        if (requestVersion == 0)
        {
            requestVersion = 40;
        }
        else
        {
            requestVersion = 0;
        }
    }
}
void GreeterServiceImpl::modelVersionThreadFunction()
{
    std::cout << "开始监听版本变换: " << std::endl;
    bool moveFlag = true;
    while (true)
    {
        // 等待字符串发生变化
        std::string modelPath = HdfsReader[0].FindModelPath(false);
        if (modelPath != nowModelVersion)
        {
            std::cout << "nowModelVersion changed: " << modelPath << std::endl;
            switchVersion = false;
            nowModelVersion = modelPath;
            versionChange(modelPath, moveFlag);
            moveFlag = false;
        }
        // std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void GreeterServiceImpl::limitQPS()
{
    while (1)
    {
        curCount = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void GreeterServiceImpl::ReadFromDisk(int sliceReaderCount, std::string localSlicePath)
{
    if (sliceReaderKeyList[sliceReaderCount] != "" && sliceReaderKeyList[sliceReaderCount] != localSlicePath)
    {
        std::cout << "内存以装载其他节点 正在卸载:" << sliceReaderKeyList[sliceReaderCount] << std::endl;
        sliceReaderList[sliceReaderCount].Unload();
        sliceReaderKeyList[sliceReaderCount] = "";
    }
    std::cout << "内存装载当前节点的SliceReader:" << sliceReaderCount << std::endl;
    sliceReaderList[sliceReaderCount].Load(localSlicePath);
    sliceReaderKeyList[sliceReaderCount] = localSlicePath;
}

Status GreeterServiceImpl::Get(ServerContext *context, const Request *request,
           Response *response) override
{


    if (request->version() == 0) // 不能阻止跨机通信的请求
    {
        curCount++;
        if (curCount > maxqps)
        {
            response->set_status(-1);
            return Status::OK;
        }
    }

    int offset = requestVersion;
    if (request->version() != 0)
    {
        offset = (request->version() - 1) * 40;
    }
    // std::cout << "offset:" << offset << std::endl;
    int processNode[10001];
    response->set_status(0);
    int i = 0;
    alimama::proto::Request requestList[7];
    std::vector<std::string> dataBufferVector[7];
    std::vector<int> position(7, 0);
    // alimama::proto::SliceRequest* slice;
    for (auto &req : request->slice_request())
    {
        processNode[i] = int(req.slice_partition() % 6) + 1;
        position[processNode[i]]++;
        alimama::proto::SliceRequest *slice = requestList[processNode[i]].add_slice_request();
        slice->set_slice_partition(req.slice_partition());
        slice->set_data_start(req.data_start());
        slice->set_data_len(req.data_len());
        if (processNode[i] == hostNode)
        {
            char data_buffer[req.data_len()];
            int sliceReaderCount = int(req.slice_partition() / 6) + offset;
            sliceReaderList[sliceReaderCount].Read(req.data_start(), req.data_len(), data_buffer);
            dataBufferVector[hostNode].emplace_back(std::string(data_buffer, req.data_len()));
        }
        i++;
    }
    if (test)
    {
        for (int i = 1; i <= 3; i += 2)
        {
            if (i != hostNode && position[i] != 0)
            {
                std::cout << "向队友节点" << i << "请求数据个数" << position[i] << std::endl;
                std::string distNode = "node-" + std::to_string(i) + ":50051";
                requestList[i].set_version(offset / 40 + 1);

                dataBufferVector[i] = greeter[i].Get(requestList[i]);
                std::cout << "请求数据成功 " << distNode << "机buffer个数：" << dataBufferVector[i].size() << std::endl;
            }
            position[i] = 0;
        }
    }
    else
    {
        for (int i = 1; i <= 6; i++)
        {
            if (i != hostNode && position[i] != 0)
            {
                if (getCout)
                {
                    std::cout << "向队友节点" << i << "请求数据个数" << position[i] << std::endl;
                }

                std::string distNode = "node-" + std::to_string(i) + ":50051";
                requestList[i].set_version(offset / 40 + 1);

                dataBufferVector[i] = greeter[i].Get(requestList[i]);
                if (getCout)
                {
                    std::cout << "请求数据成功 " << distNode << "机buffer个数：" << dataBufferVector[i].size() << std::endl;
                }
            }
            position[i] = 0;
        }
    }
    if (getCout)
    {
        if (request->version() != 0)
        {
            std::cout << "响应队友节点的访问的请求成功 返回buffer个数：" << dataBufferVector[hostNode].size() << std::endl;
        }
        else
        {

            std::cout << "本机节点访问成功 获取到本机buffer个数：" << dataBufferVector[hostNode].size() << std::endl;
        }
    }

    i = 0;
    for (auto &req : request->slice_request())
    {
        if (position[processNode[i]] >= dataBufferVector[processNode[i]].size())
        {
            std::cout << "运行出错" << processNode[i] << "节点爆了" << position[processNode[i]] << " " << dataBufferVector[processNode[i]].size() << std::endl;
            response->add_slice_data(dataBufferVector[processNode[i]][0]);
        }
        response->add_slice_data(dataBufferVector[processNode[i]][position[processNode[i]]++]);
        i++;
    }
    return Status::OK;
}