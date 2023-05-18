#include <gtest/gtest.h>
#include <regex>
#include <filesystem>
#include <fstream>
#include "map_reduce/map_reduce.hpp"


namespace fs = std::filesystem;


Job::pairs_t basic_mapper(std::string text_split) {
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
}

Job::V basic_reducer(Job::K key, std::vector<Job::V> values) {
    Job::V value = 0;
    for (auto &v : values) {
        value += v;
    }

    return value;
}

void create_file(std::vector<std::string> lines, std::string filename) {
    auto folder = fs::absolute(filename).parent_path();
    if (!fs::exists(fs::status(folder))) {
        fs::create_directories(folder);
    }
    std::ofstream file(filename, std::ios_base::out);
    for (auto &line : lines) {
        file << line << std::endl;
    };
    file.close();
}

Job::pairs_t read_map_from_file(std::string filename) {
    std::ifstream file(filename);
    std::string line;
    Job::pairs_t out;
    std::regex re("^(.+): (\\d+)$");
    std::smatch m;
    while (std::getline(file, line)) {
        std::regex_match(line, m, re);
        if (m[0].matched) {
            std::string v = m[2];
            out[m[1]] = atoi(v.c_str());
        }
    }
    file.close();
    return out;
}

class MapReduceTest : public testing::Test {
protected:
    void SetUp() override {
        fs::remove_all("testfiles");
    }

    void TearDown() override {
        fs::remove_all("testfiles");
    }
};


TEST_F(MapReduceTest, Empty) {
    {
        std::vector<std::string> in = {""};
        Job::pairs_t real_out = {};

        create_file(in, "testfiles/input.txt");

        Job j;
        j.set_input_files({"testfiles/input.txt"})
            .set_output_file("testfiles/result.txt")
            .set_tmp_folder("testfiles/tmp")
            .set_max_mappers(4)
            .set_max_reducers(4)
            .set_mapper(basic_mapper)
            .set_reducer(basic_reducer);

        j.start();

        auto out = read_map_from_file("testfiles/result.txt");
        ASSERT_EQ(real_out, out);
    }
}

TEST_F(MapReduceTest, Basic) {
    {
        std::vector<std::string> in = {
            "What determines the fate of mankind in this world?",
            "Some invisible being or law, like the Hand of the Lord hovering over the world?",
            "At least it is true that man has no power even over his own will."
        };
        Job::pairs_t real_out = {
            {"At", 1}, {"Hand", 1}, {"Lord", 1}, {"Some", 1}, {"What", 1}, {"being", 1},
            {"determines", 1}, {"even", 1}, {"fate", 1}, {"has", 1}, {"his", 1}, {"hovering", 1},
            {"in", 1}, {"invisible", 1}, {"is", 1}, {"it", 1}, {"law", 1}, {"least", 1},
            {"like", 1}, {"man", 1}, {"mankind", 1}, {"no", 1}, {"of", 2}, {"or", 1},
            {"over", 2}, {"own", 1}, {"power", 1}, {"that", 1}, {"the", 4}, {"this", 1},
            {"true", 1}, {"will", 1}, {"world", 2},
        };

        create_file(in, "testfiles/input.txt");

        Job j;
        j.set_input_files({"testfiles/input.txt"})
            .set_output_file("testfiles/result.txt")
            .set_tmp_folder("testfiles/tmp")
            .set_max_mappers(4)
            .set_max_reducers(4)
            .set_mapper(basic_mapper)
            .set_reducer(basic_reducer);

        j.start();

        ASSERT_TRUE(!fs::exists(fs::status("testfiles/tmp")));

        ASSERT_TRUE(fs::exists(fs::status("testfiles/result.txt")));
        auto out = read_map_from_file("testfiles/result.txt");
        ASSERT_EQ(real_out, out);
    }
    
    {
        std::vector<std::string> in = {
            "'Tap, tap'",
            "I hear!",
            "at the windsill,",
            "after reading all night",
            "and writing my swill",
            "",
            "'Tap' tap! ,",
            "I hear!",
            "Who could it be,",
            "For I've lost my mind,",
            "It could be anybody!",
            "",
            "Tap! tap! ,",
            "I go to the window to look,",
            "I open it slowly",
            "and I glance up",
            "",
            "Tap Tap!",
            "I behold",
            "what I cannot believe!",
            "",
            "It's my Raven",
            "",
            "And he shiting on me!",
        };
        Job::pairs_t real_out = {
            {"And", 1}, {"For", 1}, {"I", 7}, {"It", 2}, {"Raven", 1}, {"Tap", 5},
            {"Who", 1}, {"after", 1}, {"all", 1}, {"and", 1}, {"anybody", 1},
            {"at", 1}, {"be", 2}, {"behold", 1}, {"believe", 1}, {"cannot", 1},
            {"could", 2}, {"go", 1}, {"he", 1}, {"hear", 2}, {"it", 2}, {"look", 1},
            {"lost", 1}, {"me", 1}, {"mind", 1}, {"my", 3}, {"night", 1}, {"on", 1},
            {"open", 1}, {"reading", 1}, {"s", 1}, {"shiting", 1}, {"slowly", 1},
            {"swill", 1}, {"tap", 3}, {"the", 2}, {"to", 2}, {"ve", 1}, {"what", 1},
            {"window", 1}, {"windsill", 1}, {"writing", 1},
        };

        create_file(in, "testfiles/input.txt");

        Job j;
        j.set_input_files({"testfiles/input.txt"})
            .set_output_file("testfiles/result.txt")
            .set_tmp_folder("testfiles/tmp")
            .set_max_mappers(4)
            .set_max_reducers(4)
            .set_mapper(basic_mapper)
            .set_reducer(basic_reducer);

        j.start();

        ASSERT_TRUE(!fs::exists(fs::status("testfiles/tmp")));
        ASSERT_TRUE(fs::exists(fs::status("testfiles/result.txt")));
        auto out = read_map_from_file("testfiles/result.txt");
        ASSERT_EQ(real_out, out);
    }
}

TEST_F(MapReduceTest, NotEmptyingFolder) {
    {
        std::vector<std::string> in = {"some"};
        Job::pairs_t real_out = {{"some", 1}};

        create_file(in, "testfiles/input.txt");
        create_file(in, "testfiles/tmp/file.txt");

        Job j;
        j.set_input_files({"testfiles/input.txt"})
            .set_output_file("testfiles/result.txt")
            .set_tmp_folder("testfiles/tmp")
            .set_max_mappers(4)
            .set_max_reducers(4)
            .set_mapper(basic_mapper)
            .set_reducer(basic_reducer);

        j.start();

        ASSERT_TRUE(fs::exists(fs::status("testfiles/tmp/file.txt")));

        ASSERT_TRUE(fs::exists(fs::status("testfiles/result.txt")));
        auto out = read_map_from_file("testfiles/result.txt");
        ASSERT_EQ(real_out, out);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
