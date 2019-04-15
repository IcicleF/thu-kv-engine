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

namespace polar_race {
    struct Location {
        uint32_t fileNo, offset, len;
        Location(uint32_t fileNo = -1, uint32_t offset = 0, uint32_t len = 0)
            : fileNo(fileNo), offset(offset), len(len) { }
    };

    uint32_t strHash(const char* s, int size) {
        uint32_t h = 37;
        for (int i = 0; i < size; ++i)
            h = (h * 54059) ^ (s[i] * 76963);
        return h;
    }

    uint16_t getFileIndex(uint32_t hashValue) {
        return (uint16_t)(hashValue >> 16);
    }
    uint16_t getOffsetIndex(uint32_t hashValue) {
        return (uint16_t)(hashValue & 0xFFFF);
    }

    int ensureDirectory(const char* directory) {
        DIR* dir = opendir(directory);
        if (dir) {
            closedir(dir);
            return 1;
        }
        else if (errno == ENOENT) {
            mkdir(directory, 0777);
            return 0;
        }
        return -1;
    }
    int getDirFiles(const std::string& dir, std::vector<std::string>* result) {
        int res = 0;
        result->clear();
        DIR* d = opendir(dir.c_str());
        if (d == NULL) {
            return errno;
        }
        struct dirent* entry;
        while ((entry = readdir(d)) != NULL) {
            if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
            continue;
            }
            result->push_back(entry->d_name);
        }
        closedir(d);
        return res;
    }
    bool fileExists(const char* path) {
        return access(path, F_OK) == 0;
    }
    std::string pathJoin(const std::string &a, const std::string& b) {
        std::string res = a;
        if (res[res.size() - 1] != '/')
            res += '/';
        return res + b;
    }
}

#endif // ENGINE_RACE_UTILS_H
