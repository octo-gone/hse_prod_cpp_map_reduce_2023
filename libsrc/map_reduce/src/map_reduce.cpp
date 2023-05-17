#include <map_reduce/map_reduce.hpp>
#include <fstream>
#include <algorithm>
#include <atomic>
#include <thread>
#include <exception>
#include <filesystem>


namespace fs = std::filesystem;

Job& Job::set_input_files(std::vector<std::string> filenames) {
    /*
    TODO: check if they are valid
        auto status = fs::status(filename);
        fs::exists(status) && fs::is_regular_file(status)
    */
    for (auto filename : filenames) {
        _filenames.push_back(filename);
    }
    return *this;
}

Job& Job::set_tmp_folder(std::string folder) {
    /*
    TODO: if tmp folder is not set then use temp dir or use `tmpfile` for tmp files
        _tmp_folder = std::filesystem::temp_directory_path().string();
    */
    auto status = fs::status(folder);
    if (!fs::exists(status)) {
        std::cout << "`Job::set_tmp_folder`: provided folder don't exists, creating it" << std::endl;
        fs::create_directories(folder);
    } else if (!fs::is_directory(status))
        throw std::runtime_error("`Job::set_tmp_folder`: provided folder is not a directory");
    _tmp_folder = folder;
    return *this;
}

Job& Job::set_max_workers(size_t n_workers) {
    _n_workers = n_workers;
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

void show_map(std::map<std::string, size_t>& m) {
    std::cout << "Map Debug :\n";
    for (auto &el : m)
    {
        std::cout << el.first << " : " << el.second << "\n";
    }
    std::cout << std::endl;
}

void Job::start() {
    /**Flow:
    [Split stage]:
    - Сплитуем входные файлы из папки data/input/ и кладём в data/splits/
    (можно сделать многопоточно один поток на один файл)
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
    [Map stage]:
    - Перед тем как запустить мапперы расчитаем их число и выделим vector<map> вектор аккумуляторов
    - Каждый сплит это таск для маппера: !!!(1 воркер один аккум)
        - Мап_воркер1 берёт сплит1 ->(создаёт) мап_аккум1 [беря результат с _mapper(сплит1)],
                далее сплит2 ->(обновляет) мап_аккум1 [результатом _mapper(сплит2)], и т.д
        - Мап_воркер2 делает тоже самое на своём диапозоне сплитов сохраняя результат в свой мап_аккум2
        ...
        -Отработав, каждый маппер складывает свой аккум в свою ячейку в векторе аккумуляторов
    **/

    std::vector<std::vector<std::string>> global_tasks(_n_workers);
    std::vector<pairs_t> global_accum(_n_workers);

    for (size_t i = 0; i < tasks.size(); i++) {
        global_tasks[i % _n_workers].push_back(tasks[i]);
    }

    auto thread_work = [&global_accum](size_t n, mapper_t mapper, std::vector<std::string> worker_tasks) {
        std::map<std::string, size_t> accum{};
        std::map<std::string, size_t> map_result{};
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

    std::vector<std::thread> ts(_n_workers);

    for (size_t i = 0; i < _n_workers; i++) {
        std::thread t(thread_work, i, _mapper, global_tasks[i]);
        ts.push_back(std::move(t));
    }

    /**
    [Sync]
    **/
    size_t joined_cnt = 0;
    while (joined_cnt < _n_workers) {
        for (auto &t : ts) {
            if (t.joinable()) {
                t.join();
                joined_cnt++;
            }
        }
    }

    // DEBUG :
    // for (auto i: global_accum) {
    //     std::cout << "--" << std::endl;
    //     for (auto j: i) {
    //         std::cout << j.first << " : " << j.second << std::endl;
    //     }
    // }

    for (auto task_filename: tasks) {
        fs::remove(task_filename);
    }
    std::error_code ec;
    fs::remove(_tmp_folder, ec);
    /**
    [Shuffle stage]
    - Собираем новый vector<map> в котором len = [количество редьюсеров], группировка по ключам
            Количество всех уникальных ключей по всем аккумам - K_all
            Количество редьюсеров - R
            Количество групп ключей на одном редьюсере ceil(K_all / R), остаток групп распределяется

    **/
}