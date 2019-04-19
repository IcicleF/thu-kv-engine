#if !defined(ENGINE_RACE_DATA_STORE_H)
#define ENGINE_RACE_DATA_STORE_H

#include "utils.h"

#define STORE_FILESZ (1 << 26)

/*
 * Structure of a persistent record:
 *  - Key length        (4 bytes)
 *  - Value length      (4 bytes)
 *  - Next (File ID)    (4 bytes)
 *  - Next (Offset)     (4 bytes)
 *  - Key
 *  - Value
 */

namespace polar_race {
    struct RecordMeta {
        uint32_t keyLen;
        uint32_t valLen;
        int32_t nextFileNo;
        uint32_t nextOffset;

        RecordMeta() : keyLen(0), valLen(0), nextFileNo(-1), nextOffset(0) { }
    };

    class DataStore {
        public:
            explicit DataStore(const std::string& root) : dir(pathJoin(root, "data")) { }
            ~DataStore() { }

            void init();

            RetCode readData(const char *key, int keyLen, char *value, Location *loc);
            RetCode getWriteLocation(int keyLen, int valLen, Location *loc);

        private:
            bool findLocation(const char *key, int keyLen, Location *loc);

        private:
            std::string dir;
            int32_t lastNo;
    };
}

#endif // ENGINE_RACE_DATA_STORE_H
