#include "map_reduce/map_reduce.hpp"
#include <regex>

int main(int argc, char *argv[]) {
    if (argc < 4) {
        throw std::runtime_error("`main`: not enough arguments");
    }
    std::vector<std::string> input_files;
    std::string tmp_folder, output_file;
    for (auto i = 1; i < argc; i++) {
        if (i == argc - 1) {
            output_file = argv[i];
        }
        else if (i == argc - 2) {
            tmp_folder = argv[i];
        }
        else
            input_files.push_back(argv[i]);
    }
    Job j;
    j.set_input_files(input_files)
        .set_output_file(output_file)
        .set_tmp_folder(tmp_folder)
        .set_max_mappers(4)
        .set_max_reducers(4)
        .set_mapper([](std::string text_split) {
            Job::pairs_t pair_accum{};

            std::regex word_regex("(\\b\\w+\\b)");
            auto words_begin = std::sregex_iterator(text_split.begin(), text_split.end(), word_regex);
            auto words_end = std::sregex_iterator();
            std::string word;
            for (std::sregex_iterator i = words_begin; i != words_end; ++i) {
                std::smatch match = *i;
                std::string word = match.str();
                if (pair_accum.find(word) != pair_accum.end())
                    pair_accum[word] += 1;
                else
                    pair_accum[word] = 1;
            }

            return pair_accum;
        })
        .set_reducer([](Job::K key, std::vector<Job::V> values) {
            Job::V value = 0;
            for (auto &v : values) {
                value += v;
            }

            return value;
        });

    j.start();
    return 0;
}