#include "map_reduce/map_reduce.hpp"

int main() {
    Job j;
    j.set_input_files({"../examples/war_and_peace.txt"})
        .set_tmp_folder("../bin/split")
        .set_max_mappers(4)
        .set_max_reducers(4)
        .set_mapper([](std::string text_split) {
            std::map<std::string, size_t> pair_accum{};

            // splits by any space char
            std::stringstream ss(text_split);
            std::string word;

            while (ss >> word) {
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