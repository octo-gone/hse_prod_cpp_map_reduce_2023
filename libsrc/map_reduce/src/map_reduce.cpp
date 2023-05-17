#include <map_reduce/map_reduce.hpp>
#include <fstream>
#include <algorithm>
#include <atomic>
#include <thread>
#include <exception>
#include <filesystem>


namespace fs = std::filesystem;


Job& Job::set_input_files(std::vector<std::string> filenames) {
    for (auto filename : filenames) {
        auto status = fs::status(filename);
        if (!fs::exists(status) && !fs::is_regular_file(status))
#ifndef NDEBUG
            std::cout << "`Job::set_input_files`: provided filename '"
                + filename + "' don't exist or is not a regular file, excluding it" << std::endl;
#else
            throw std::runtime_error("`Job::set_input_files`: provided filename '"
                + filename + "' don't exist or is not a regular file, excluding it");
#endif
        else
            _filenames.push_back(filename);
    }
    return *this;
}

Job& Job::set_tmp_folder(std::string folder) {
    auto status = fs::status(folder);
    if (!fs::exists(status)) {
#ifndef NDEBUG
        std::cout << "`Job::set_tmp_folder`: provided folder don't exist, creating it" << std::endl;
#else
        throw std::runtime_error("`Job::set_tmp_folder`: provided folder don't exist, creating it");
#endif
        fs::create_directories(folder);
    } else if (!fs::is_directory(status))
        throw std::runtime_error("`Job::set_tmp_folder`: provided folder is not a directory");
    _tmp_folder = folder;
    return *this;
}

Job& Job::set_max_mappers(size_t n_mappers) {
    _n_mappers = n_mappers;
    return *this;
}

Job& Job::set_max_reducers(size_t n_reducers) {
    _n_reducers = n_reducers;
    return *this;
}

Job& Job::set_mapper(Job::mapper_t callback) {
    _mapper = callback;
    return *this;
}

Job& Job::set_reducer(Job::reducer_t callback) {
    _reducer = callback;
    return *this;
}

void show_map(std::map<Job::K, Job::V>& m) {
    std::cout << "{" << std::endl;
    for (auto &el : m)
    {
        std::cout << "  \"" << el.first << "\" : " << el.second << "," << std::endl;
    }
    std::cout << "}" << std::endl;
}

void Job::start() {
    /**
    [Split stage]
    **/

    size_t file_cnt = 0;
    std::vector<std::string> tasks;
    for (auto filename : _filenames) {
        std::ifstream file(filename);
        size_t lines_count = std::count(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>(), '\n') + 1;
        size_t split_size = std::max(lines_count / 10, 15ul);

        file.seekg(0, std::ios::beg);
        std::string line;

        size_t cnt = 0;
        size_t split_cnt = 0;
        std::ofstream split_file;
        std::string split_filename;
        while (std::getline(file, line)) {
            if (cnt == 0) {
                split_filename = _tmp_folder + "/file_" + std::to_string(file_cnt) + "_" + std::to_string(split_cnt) + ".txt";
                split_file.open(split_filename, std::ios_base::out);
            }
            if (cnt++ == split_size) {
                tasks.push_back(split_filename);
                split_file.close();
                split_cnt++;
                cnt = 0;
            }
            split_file << line << std::endl;
        }
        tasks.push_back(split_filename);
        split_file.close();
        file.close();
        file_cnt++;
    }
    /**
    [Map stage]
    **/

    std::vector<std::vector<std::string>> mappers_tasks(_n_mappers);
    std::vector<pairs_t> global_accum(_n_mappers);

    for (size_t i = 0; i < tasks.size(); i++) {
        mappers_tasks[i % _n_mappers].push_back(tasks[i]);
    }

    auto mapper_work = [&global_accum](size_t n, mapper_t mapper, std::vector<std::string> worker_tasks) {
        std::map<K, V> accum{};
        std::map<K, V> map_result{};
        for (auto filename: worker_tasks) {
            std::ifstream file(filename);
            std::string line, split;
            while (std::getline(file, line)) {
                if (split.empty()) split = line;
                else split += " " + line;
            }
            file.close();

            map_result = mapper(split);
            for (auto &pair : map_result) {
                auto key = pair.first;
                auto value = pair.second;
                if (accum.find(key) != accum.end())
                    accum[key] += value;
                else
                    accum[key] = value;
            }
        }
        global_accum[n] = std::move(accum);
    };

    std::vector<std::thread> ts(_n_mappers);

    for (size_t i = 0; i < _n_mappers; i++) {
        std::thread t(mapper_work, i, _mapper, mappers_tasks[i]);
        ts.push_back(std::move(t));
    }

    /**
    [Sync]
    **/
    size_t joined_cnt = 0;
    while (joined_cnt < _n_mappers) {
        for (auto &t : ts) {
            if (t.joinable()) {
                t.join();
                joined_cnt++;
            }
        }
    }

    /**
    [Shuffle stage]
    **/
    pairs_lists_t shuffled_accum;

    for (auto accum: global_accum) {
        for (auto pair: accum) {
            shuffled_accum[pair.first].push_back(pair.second);
        }
    }

    std::vector<pairs_lists_t> reducers_tasks(_n_reducers);
    size_t key_cnt = 0;

    for (auto pair: shuffled_accum) {
        reducers_tasks[key_cnt % _n_reducers][pair.first] = std::move(pair.second);
        key_cnt++;
    }

    /**
    [Reducing]
    **/
    pairs_t result;

    auto reducer_work = [&result](reducer_t reducer, pairs_lists_t worker_task) {
        for (auto pair: worker_task) {
            result[pair.first] = reducer(pair.first, pair.second);
        }
    };

    ts = std::vector<std::thread>(_n_reducers);

    for (size_t i = 0; i < _n_reducers; i++) {
        std::thread t(reducer_work, _reducer, reducers_tasks[i]);
        ts.push_back(std::move(t));
    }

    /**
    [Sync]
    **/
    joined_cnt = 0;
    while (joined_cnt < _n_reducers) {
        for (auto &t : ts) {
            if (t.joinable()) {
                t.join();
                joined_cnt++;
            }
        }
    }

    /*
    [Cleanup]
    */
    for (auto task_filename: tasks) {
        fs::remove(task_filename);
    }
    std::error_code ec;
    fs::remove(_tmp_folder, ec);
}