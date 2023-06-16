#include "EtcdUtil.hpp"

void EtcdUtil::writeData(const std::string &key, const std::string &value)
{
    etcd::Response response = myetcd.set(key, value).get();

    if (response.is_ok())
    {
        std::cout << "etcd写入成功" << std::endl;
    }
    else
    {
        std::cerr << "etcd写入失败: " << response.error_message() << std::endl;
    }
}

std::string EtcdUtil::readData(const std::string &key)
{
    pplx::task<etcd::Response> responseTask = myetcd.get(key);
    responseTask.wait(); // 等待任务完成

    etcd::Response response = responseTask.get();

    if (response.is_ok())
    {
        return response.value().as_string();
    }
    else
    {
        return "";
    }
}

void EtcdUtil::regist()
{
    std::string external_address = getLocalIP() + std::string(":") + std::to_string(port);
    std::string key = std::string("/services/modelservice/") + external_address;
    writeData(key, external_address);
    // 维护当前版本信息
}

std::string EtcdUtil::getLoadAverage()
{
    std::ifstream loadFile("/proc/loadavg");
    std::string loadAverage;
    if (loadFile.is_open())
    {
        std::getline(loadFile, loadAverage);
        loadFile.close();
    }
    return loadAverage;
}