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
    Job& set_input_files(std::vector<std::string>);
    Job& set_tmp_folder(std::string);
    Job& set_max_workers(size_t);
    Job& set_mapper(mapper_t);
    Job& set_reducer(reducer_t);
    void start();
};
