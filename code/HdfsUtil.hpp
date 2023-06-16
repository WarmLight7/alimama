#include <string>
#include <vector>



class HdfsUtil {
public:
    std::vector<std::string> splitString(const std::string& str, char delimiter);

    std::string splitLastString(const std::string& str, char delimiter);

    bool cmp(std::string & a, std::string & b);

    bool directoryExists(const std::string& folderPath);

    bool createDirectory(const std::string& folderPath);

    std::string FindSlicePath(std::string modelPath, int slice_partition, int sliceLenth);
};

