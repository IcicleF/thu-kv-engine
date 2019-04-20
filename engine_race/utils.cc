#include "utils.h"

namespace polar_race {
    uint32_t strHash(const char* s, int size) {
        uint32_t h = 37;
        for (int i = 0; i < size; ++i)
            h = (h * 54059) ^ (s[i] * 76963);
        return h & (ENTRIES_COUNT - 1);
    }

    int ensureDirectory(const std::string &directory) {
        DIR* dir = opendir(directory.c_str());
        if (dir) {
            closedir(dir);
            return 1;
        }
        else if (errno == ENOENT) {
            mkdir(directory.c_str(), 0777);
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
            if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0)
                continue;
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