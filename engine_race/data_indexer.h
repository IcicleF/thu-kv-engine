#if !defined(ENGINE_RACE_DATA_INDEXER_H)
#define ENGINE_RACE_DATA_INDEXER_H

#include "utils.h"
#include <sstream>

#define LOC_READ        0
#define LOC_WRITE       1

#define BASE_KEYFILESZ  (1 << 20)

/*
 * Structure of index file:
 *  - offset pointer    (* ENTRIES_COUNT, sizeof(off_t) each)
 *  - linked list
 *   - (for each item)
 *    - key length      (4 bytes)
 *    - fileNo          (4 bytes)
 *    - offset          (4 bytes)
 *    - next offset     (sizeof(off_t))
 *    - key
 */

namespace polar_race {
    struct IndexItem {
        uint32_t keyLen;
        uint32_t fileNo;
        uint32_t offset;
        off_t next;
    };

    class DataIndexer {
        public:
            explicit DataIndexer(const std::string& root) : dir(root), locs(NULL), keys(NULL) { }
            ~DataIndexer() {
                if (locs != NULL)
                    munmap(locs, INDEX_MAPSZ);
                if (keys != NULL)
                    munmap(keys, keyFileLen);
            }

            RetCode init();
            RetCode find(const char *key, int keyLen, Location *loc);
            RetCode insert(const char *key, int keyLen, Location loc);
            void syncIndex();

        private:
            std::string dir;
            
            off_t *locs;
            char *keys;
            size_t keyFileLen, *usedLen;
    };
}

#endif // ENGINE_RACE_DATA_INDEXER_H
