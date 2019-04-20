#if !defined(ENGINE_RACE_DATA_STORE_H)
#define ENGINE_RACE_DATA_STORE_H

#include "utils.h"

#define STORE_FILESZ (1 << 26)

/*
 * Structure of a persistent record:
 *  - Key length        (4 bytes)
 *  - Value length      (4 bytes)
 *  - Key
 *  - Value
 */

namespace polar_race {
    class DataStore {
        public:
            explicit DataStore(const std::string& root) : dir(pathJoin(root, "data")) {

            }
            ~DataStore() {
                munmap(ptr, STORE_FILESZ);
            }

            void init();

            RetCode readData(char **value, int *valLen, Location loc);
            RetCode writeData(const char *key, int keyLen, const char *value, int valLen);
            void syncData();
            void getWriteLocation(Location *loc);

        private:
            std::string dir;
            int32_t lastNo;
            int fd;
            void *ptr;
            char *fp, *fend, *flast;
    };
}

#endif // ENGINE_RACE_DATA_STORE_H
