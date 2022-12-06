#include <gtest/gtest.h>

#include <string.h>

#include "MapReduce.h"

TEST(MapReduceTest, TestPopulateWordMap_BasicPass) {
    std::map<std::string, int> wordMap;
    std::string line = "Welcome to the Jungle";
    populateWordMap(line, wordMap);
    EXPECT_EQ(wordMap["Welcome"], 1);
    EXPECT_EQ(wordMap["to"], 1);
    EXPECT_EQ(wordMap["the"], 1);
    EXPECT_EQ(wordMap["Jungle"], 1);
}

TEST(MapReduceTest, TestPopulateWordMap_BasicFail) {
    std::map<std::string, int> wordMap;
    std::string line = "Welcome to the Jungle";
    populateWordMap(line, wordMap);
    EXPECT_EQ(wordMap["NOPE"], 0) << "Word not from original line found in map";
}

// this is probably not a repeatable test if the hash is not seeded...
TEST(MapReduceTest, TestGetReducerQueueId_BasicPass) {
    std::string word = "Jungle";
    std::hash<std::string> wordHashFn;
    const int maxReducers = 10;

    int reducerQueueId = getReducerQueueId(word, wordHashFn, maxReducers);
    EXPECT_EQ(reducerQueueId, 1);
}

TEST(MapReduceTest, TestAddTestFiles_BasicPass) {
    std::queue<std::string> testFiles;

    // addTestFiles("../test/files", testFiles);
    EXPECT_EQ(testFiles.front(), "../test/files/1.txt");
}