#include <iostream>
#include <fstream>
#include <string>
#include <etcd/Client.hpp>

class EtcdUtil {
private:
    etcd::Client myetcd;
    int port = 8080; // 你的端口号

public:
    EtcdUtil() : myetcd("http://localhost:2379") {}

    void writeData(const std::string& key, const std::string& value);

    std::string readData(const std::string& key);

    void regist();

    std::string getLoadAverage();


};

