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

            RetCode getLocation(uint32_t index, Location *loc);
        
        private:
            std::string dir;
    };
}

#endif // ENGINE_RACE_DATA_INDEXER_H
