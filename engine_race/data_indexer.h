#if !defined(ENGINE_RACE_DATA_INDEXER_H)
#define ENGINE_RACE_DATA_INDEXER_H

#include "utils.h"
#include <sstream>

#define ENTRIES_COUNT   (1 << 16)
#define MAP_SIZE        (ENTRIES_COUNT * sizeof(Location))

#define LOC_READ        0
#define LOC_WRITE       1

namespace polar_race {
    class DataIndexer {
        public:
            explicit DataIndexer(const std::string& root) : dir(pathJoin(root, "index")) { }
            ~DataIndexer() { }

            RetCode getLocation(uint32_t index, Location *loc) {
                return operLocation(index, loc, LOC_READ);
            }
            RetCode setLocation(uint32_t index, Location loc) {
                return operLocation(index, &loc, LOC_WRITE);
            }
        
        private:
            RetCode operLocation(uint32_t index, Location *loc, int opType) {
                uint16_t fileNo = getFileIndex(index);
                uint16_t itemID = getOffsetIndex(index);

                std::stringstream ss;
                ss << fileNo;
                std::string indexFile = pathJoin(dir, ss.str());
                
                bool new_create = false;
                int fd = open(indexFile.c_str(), O_RDWR, 0777);
                if (fd < 0 && errno == ENOENT) {
                    fd = open(indexFile.c_str(), O_RDWR | O_CREAT, 0777);
                    if (fd >= 0) {
                        new_create = true;
                        if (posix_fallocate(fd, 0, MAP_SIZE) != 0) {
                            close(fd);
                            return kIOError;
                        }
                    }
                }
                if (fd < 0)
                    return kIOError;
                
                void* ptr = mmap(NULL, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                if (ptr == MAP_FAILED) {
                    close(fd);
                    return kIOError;
                }
                Location *locs = reinterpret_cast<Location *>(ptr);
                if (new_create) {
                    for (int i = 0; i < ENTRIES_COUNT; ++i) {
                        locs[i].fileNo = -1;
                        locs[i].len = 0;
                        locs[i].offset = 0;
                    }
                    fsync(fd);
                }

                RetCode ret = kSucc;
                if (opType == LOC_READ)
                    *loc = locs[itemID];
                else if (opType == LOC_WRITE) {
                    /* atomicity? */
                    locs[itemID] = *loc;
                    fsync(fd);
                }
                else
                    ret = kInvalidArgument;locs[itemID] = *loc;

                munmap(ptr, MAP_SIZE);
                close(fd);
                return ret;
            }
        
        private:
            std::string dir;
    };
}

#endif // ENGINE_RACE_DATA_INDEXER_H
