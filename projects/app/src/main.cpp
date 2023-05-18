#include "map_reduce/map_reduce.hpp"
#include <regex>

int main() {
    Job j;
    j.set_input_files({"../examples/war_and_peace.txt"})
        .set_output_file("../output/result.txt")
        .set_tmp_folder("../output/split")
        .set_max_mappers(4)
        .set_max_reducers(4)
        .set_mapper([](std::string text_split) {
            std::map<std::string, size_t> pair_accum{};

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
        .set_reducer([](std::string key, std::vector<size_t> values) {
            size_t value = 0;
            for (auto &v : values) {
                value += v;
            }

            return value;
        });

    j.start();
    return 0;
}