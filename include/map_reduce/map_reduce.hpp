#include <functional>
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <map>

class Job {
public:
    using pairs_t = std::map<std::string, size_t>;
    using mapper_t = std::function<pairs_t(std::string)>;
    using reducer_t = std::function<pairs_t(pairs_t)>;

private:
    std::vector<std::string> _filenames = {};
    std::string _tmp_folder;
    size_t _n_workers;
    mapper_t _mapper;
    reducer_t _reducer;

public:
    Job &setInputFiles(std::vector<std::string>);
    Job &setTmpFolder(std::string);
    Job &setMaxWorkers(size_t);
    Job &setMapper(mapper_t);
    Job &setReducer(reducer_t);
    void start();
};
