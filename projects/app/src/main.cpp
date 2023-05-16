#include "map_reduce/map_reduce.hpp"

int main() {
    Job j;
    j.setInputFiles({"../examples/file_001.txt", "../examples/file_002.txt"})
        .setTmpFolder("../bin/splits")
        .setMaxWorkers(4)
        .setMapper([](std::string text_split)
        {
            std::map<std::string, size_t> pair_accum;

            // splits by any space char
            std::stringstream ss(text_split);
            std::string word;

            while (ss >> word)
            {
                if (pair_accum.find(word) != pair_accum.end())
                    pair_accum[word] += 1;
                else
                    pair_accum[word] = 1;
            }

            return pair_accum;
        })
        .setReducer([](Job::pairs_t pairs)
        {
            std::map<std::string, size_t> pair_accum;
            for (auto &pair : pairs)
            {
                auto key = pair.first;
                auto value = pair.second;
                if (pair_accum.find(key) != pair_accum.end())
                    pair_accum[key] += value;
                else
                    pair_accum[key] = value;
            }

            return pair_accum;
        });

    j.start();
    return 0;
}