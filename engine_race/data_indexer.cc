#include "data_indexer.h"

namespace polar_race {
    RetCode DataIndexer::getLocation(uint32_t index, Location *loc) {
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
        
        void *ptr = mmap(NULL, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
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
        *loc = locs[itemID];
        
        munmap(ptr, MAP_SIZE);
        close(fd);
        return kSucc;
    }
}