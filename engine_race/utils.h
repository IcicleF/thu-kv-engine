#if !defined(ENGINE_RACE_UTILS_H)
#define ENGINE_RACE_UTILS_H

#include <dirent.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/mman.h>

#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <cstdint>
#include <vector>
#include <string>

#include "include/engine.h"
#include "include/polar_string.h"

#define MAX_KEYSZ (1 << 10)
#define MAX_VALSZ (1 << 22)

#define ENTRIES_COUNT (1 << 25)
#define INDEX_MAPSZ   (ENTRIES_COUNT * sizeof(off_t))

#define bufWrite(buf, offs, src, len) do { memcpy(buf + offs, (void *)(src), len); offs += len; } while(0)
#define bufRead(ptr, type) (([](void *&p) -> type { p += sizeof(type); return *((type *)(p - sizeof(type))); })(ptr))

namespace polar_race {
    struct Location {
        int32_t fileNo;
        uint32_t offset;

        Location(int32_t fileNo = -1, uint32_t offset = 0) : fileNo(fileNo), offset(offset) { }
    };

    struct RecoveredLog {
        int keyLen, valLen;
        char *key, *value;

        RecoveredLog(char *key = NULL, int keyLen = 0, char *value = NULL, int valLen = 0)
            : key(key), keyLen(keyLen), value(value), valLen(valLen) { }
    };

    uint32_t strHash(const char* s, int size);

    uint32_t getFileIndex(uint32_t hashValue);
    uint32_t getOffsetIndex(uint32_t hashValue);

    int ensureDirectory(const std::string &directory);
    int getDirFiles(const std::string& dir, std::vector<std::string>* result);
    bool fileExists(const char* path);
    std::string pathJoin(const std::string &a, const std::string& b);
}

#endif // ENGINE_RACE_UTILS_H
