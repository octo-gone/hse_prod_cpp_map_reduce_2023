#include <gtest/gtest.h>
#include "map_reduce/map_reduce.hpp"


TEST(MapReduceTest, TestTest) {
    {
        ASSERT_EQ(0, 0);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}