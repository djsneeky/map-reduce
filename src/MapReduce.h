#ifndef MAP_REDUCE_H_
#define MAP_REDUCE_H_

#include <map>
#include <string>
#include <cstdbool>
#include <queue>
#include <omp.h>
#include <chrono>

static const int MAX_NODES = 16;

struct line_queue_t
{
    line_queue_t()
        : filled(false)
    {
    }
    std::vector<std::string> line;
    omp_lock_t lock;
    bool filled;
};

struct reducer_queue_t
{
    std::queue<std::pair<std::string, int>> wordQueue;
    omp_lock_t lock;
    bool filled;
};


enum MessageType
{
    REQUEST_NEW_FILE = 0,
    NEW_FILE = 1, //geting lazy
    NO_MORE_FILES = 2,
    REDUCER_QUEUE = 3,
    MAPPER_DONE = 4,
    ALL_MAPPERS_DONE = 5
};


struct MessageHeader
{
    MessageType messageId;
    int source;
    int pack(char* buffer);
    int unpack(char* buffer);
    const int getSize();
};

struct SendBufferEntry
{
    char* buffer;
    int size;
    int dest;
};
struct mpiState
{
    bool outOfFiles;
    bool messageSent;
    volatile bool allMappersDone;
    bool outOfFileSentMasterList[MAX_NODES];
    bool mappersDoneReceivedList[MAX_NODES];
    int outOfFileSentMaster;
    int mappersDoneReceived;
    bool masterMapperDone;
};


void mapReduceParallel(int readerThreadCount, int mapperThreadCount, int reducerThreadCount, std::string outputFileName, int repeatFiles);
bool mapReduceSerial();
unsigned int getReducerQueueId(const std::string &word, const std::hash<std::string> &wordHashFn, const unsigned int maxReducers);
double readerTask(std::vector<std::string> &testFileList, std::vector<line_queue_t *> &lineQueues, volatile int *readersDone);
double mapperTask(std::vector<line_queue_t *> &lineQueues,
                                         std::map<std::string, int> &wordMap,
                                         std::vector<reducer_queue_t *> &reducerQueues,
                                         int reducerCount,
                                         const std::hash<std::string> &wordHashFn,
                                         volatile int *readersDone,
                                         volatile int *mappersDone);
double reducerTask(reducer_queue_t *reducerQueue, std::map<std::string, int> &reducerMap, volatile int *mappersDone, volatile int *reducersDone);
void writeOutFile(std::vector<std::map<std::string, int>> &reducerMaps, std::ofstream &output);
void writeOutFile(std::map<std::string, int> &reducerMaps, std::ofstream &output);
void writeOutReducersConsole(std::vector<std::map<std::string, int>> &reducerMaps);
bool populateLineQueues(const std::string &fileName, std::vector<std::queue<std::string>> &lineQueues);
bool populateLineQueue(const std::string& fileName, std::queue<std::string>& lineQueue);
bool populateLineQueue(const std::string &fileName, line_queue_t* lineQueue);
void populateWordMap(std::string& line, std::map<std::string, int>& wordMap);
void populateWordMap(std::string &line, std::vector<std::map<std::string, int>>& localMaps, const std::hash<std::string> &wordHashFn, int reducerCount);
void addTestFiles(const std::string &dirPath, std::queue<std::string> &testFiles);

// MPI
void mapReduceMPIParallel(int readerThreadCount, int mapperThreadCount, int reducerThreadCount, std::string outputFileName, int repeatFiles);

double readerTaskMpi(std::vector<std::string> &testFileList, std::vector<line_queue_t *> &lineQueues, volatile int *readersDone);

double mapperTaskMpi(std::vector<line_queue_t *> &lineQueues,
                  std::vector<reducer_queue_t *> &reducerQueues,
                  int reducerCount,
                  const std::hash<std::string> &wordHashFn,
                  volatile int *readersDone,
                  volatile int *mappersDone);

double reducerTaskMpi(reducer_queue_t *reducerQueue,
                    std::map<std::string, int> &reducerMap,
                    volatile bool *mappersDone,
                    volatile int *reducersDone);

void populateWordMapMpi(std::string &line,
                        std::vector<std::vector<std::map<std::string, int>>>& localMaps,
                        const std::hash<std::string> &wordHashFn,
                        int reducerCount);


int getMapSize(std::map<std::string,int>& map);
void serializeMap(char* buffer, std::map<std::string,int>& map);
void unserializeMap(char* buffer, int bufferLength, std::map<std::string,int>& map);



bool noMoreFilesSentToAllNodes();

bool allMappersDone();

void sendAllMappersDone();

void sendNoMoreFilesToNodes();

#endif