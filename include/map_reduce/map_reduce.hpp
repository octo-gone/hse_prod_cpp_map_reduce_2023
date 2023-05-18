#include <functional>
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <map>

class Job {
public:
    using K = std::string;
    using V = size_t;
    using pairs_t = std::map<K, V>;
    using pairs_lists_t = std::map<K, std::vector<V>>;
    using mapper_t = std::function<pairs_t(K)>;
    using reducer_t = std::function<size_t(K, std::vector<V>)>;

private:
    std::vector<std::string> _filenames = {};
    std::string _output_file;
    std::string _tmp_folder;
    size_t _n_mappers;
    size_t _n_reducers;
    mapper_t _mapper;
    reducer_t _reducer;

public:
    Job& set_input_files(std::vector<std::string>);
    Job& set_output_file(std::string);
    Job& set_tmp_folder(std::string);
    Job& set_max_mappers(size_t);
    Job& set_max_reducers(size_t);
    Job& set_mapper(mapper_t);
    Job& set_reducer(reducer_t);
    void start();

private:
    void write_map_to_file(std::map<Job::K, Job::V>&, std::string);
};

void show_map(std::map<Job::K, Job::V>&);
