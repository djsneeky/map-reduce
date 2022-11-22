#include <gtest/gtest.h>

#include <string.h>

#include "MapReduce.h"

// Demonstrate some basic assertions.
TEST(HelloTest, BasicAssertions) {
  // Expect two strings not to be equal.
  EXPECT_STRNE("hello", "world");
  // Expect equality.
  EXPECT_EQ(7 * 6, 42);
}

TEST(MapReduceTest, TestPopulateWordMap_BasicPass) {
    std::map<std::string, int> wordMap;
    std::string line = "Welcome to the Jungle";
    populateWordMap(line, wordMap, ' ');
    EXPECT_EQ(wordMap["Welcome"], 1);
    EXPECT_EQ(wordMap["to"], 1);
    EXPECT_EQ(wordMap["the"], 1);
    EXPECT_EQ(wordMap["Jungle"], 1);
}

TEST(MapReduceTest, TestPopulateWordMap_BasicFail) {
    std::map<std::string, int> wordMap;
    std::string line = "Welcome to the Jungle";
    populateWordMap(line, wordMap, ' ');
    EXPECT_EQ(wordMap["NOPE"], 0) << "Word not from original line found in map";
}