#include <map_reduce/map_reduce.hpp>
#include <fstream>
#include <algorithm>
#include <atomic>
#include <thread>

Job& Job::setInputFiles(std::vector<std::string> filenames) {
    for (auto filename : filenames) {
        _filenames.push_back(filename);
    }
    return *this;
}

Job& Job::setTmpFolder(std::string folder) {
    _tmp_folder = folder;
    return *this;
}

Job& Job::setMaxWorkers(size_t n_workers) {
    _n_workers = n_workers;
    return *this;
}

Job& Job::setMapper(Job::mapper_t callback) {
    _mapper = callback;
    return *this;
}

Job& Job::setReducer(Job::reducer_t callback) {
    _reducer = callback;
    return *this;
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
        pairs_t accum;
        for (auto filename: worker_tasks) {
            std::ifstream file(filename);
            std::string line;
            while (std::getline(file, line)) {
                // if (accum.empty())
                //     accum = mapper(line);
                // else {
                // }
            }
            file.close();
        }
        global_accum[n] = accum;
    };

    std::vector<std::thread> ts(_n_workers);
    for (size_t i = 0; i < _n_workers; i++) {
        std::thread t(thread_work, i, _mapper, global_tasks[i]);
    }

    for (size_t i = 0; i < _n_workers; i++) {
        ts[i].join();
    }

    for (auto i: global_accum) {
        std::cout << "--" << std::endl;
        for (auto j: i) {
            std::cout << j.first << std::endl;
        }
    }

    /**
    [Sync]
    [Shuffle stage]
    - Собираем новый vector<map> в котором len = [количество редьюсеров], группировка по ключам
            Количество всех уникальных ключей по всем аккумам - K_all
            Количество редьюсеров - R
            Количество групп ключей на одном редьюсере ceil(K_all / R), остаток групп распределяется

    **/
}