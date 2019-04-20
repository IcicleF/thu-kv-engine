#include "data_indexer.h"

namespace polar_race {
    RetCode DataIndexer::init() {
        std::string indexFile = pathJoin(dir, "index");
        bool new_create = false;

        int fd = open(indexFile.c_str(), O_RDWR, 0777);
        if (fd < 0 && errno == ENOENT) {
            fd = open(indexFile.c_str(), O_RDWR | O_CREAT, 0777);
            if (fd < 0)
                return kIOError;
            new_create = true;
            posix_fallocate(fd, 0, INDEX_MAPSZ);
        }
        void *ptr = mmap(NULL, INDEX_MAPSZ, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);
        if (ptr == MAP_FAILED)
            return kIOError;
        locs = (off_t *)ptr;
        if (new_create) {
            memset(locs, -1, INDEX_MAPSZ);
            msync(ptr, INDEX_MAPSZ, MS_ASYNC);
        }

        std::string keyFile = pathJoin(dir, "keys");
        new_create = false;
        if (!fileExists(keyFile.c_str())) {
            keyFileLen = BASE_KEYFILESZ;
            new_create = true;
        }
        else {
            struct stat stat_buf;
            stat(keyFile.c_str(), &stat_buf);
            keyFileLen = stat_buf.st_size;
        }
        fd = open(keyFile.c_str(), O_RDWR | O_CREAT, 0777);
        if (fd < 0)
            return kIOError;
        posix_fallocate(fd, 0, keyFileLen);
        ptr = mmap(NULL, keyFileLen, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);
        if (ptr == MAP_FAILED)
            return kIOError;
        keys = (char *)ptr;
        usedLen = (size_t *)keys;
        if (new_create)
            *usedLen = sizeof(size_t);
        return kSucc;
    }
    
    // to be modified
    RetCode DataIndexer::insert(const char *key, int keyLen, Location loc) {
        uint32_t index = strHash(key, keyLen);
        off_t ptr = locs[index], prev = -1;
        IndexItem *ind;
        while (ptr != -1) {
            ind = (IndexItem *)(keys + ptr);
            if (keyLen == ind->keyLen) {
                if (memcmp(keys + ptr + sizeof(IndexItem), key, keyLen) == 0)
                    break;
            }
            prev = ptr;
            ptr = ind->next;
        }
        if (prev != -1 && ptr != -1) {
            IndexItem *ind_prev = (IndexItem *)(keys + prev);
            ind_prev->next = ind->next;
        }

        size_t newUsedLen = *usedLen + sizeof(IndexItem) + keyLen;
        if (newUsedLen > keyFileLen) {
            munmap(keys, keyFileLen);
            std::string keyFile = pathJoin(dir, "keys");
            int fd = open(keyFile.c_str(), O_RDWR, 0777);
            while (newUsedLen > keyFileLen) {
                posix_fallocate(fd, keyFileLen, keyFileLen);
                keyFileLen *= 2;
            }
            void *ptr = mmap(NULL, keyFileLen, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            keys = (char *)ptr;
            usedLen = (size_t *)keys;
            close(fd);
        }
        
        ind = (IndexItem *)(keys + *usedLen);
        ind->keyLen = keyLen;
        ind->fileNo = loc.fileNo;
        ind->offset = loc.offset;
        ind->next = locs[index];
        memcpy(keys + *usedLen + sizeof(IndexItem), key, keyLen);
        locs[index] = *usedLen;
        *usedLen = newUsedLen;

        return kSucc;
    }

    RetCode DataIndexer::find(const char *key, int keyLen, Location *loc) {
        uint32_t index = strHash(key, keyLen);
        off_t ptr = locs[index];
        IndexItem *ind;
        while (ptr != -1) {
            ind = (IndexItem *)(keys + ptr);
            if (keyLen == ind->keyLen)
                if (memcmp(keys + ptr + sizeof(IndexItem), key, keyLen) == 0) {
                    loc->fileNo = ind->fileNo;
                    loc->offset = ind->offset;
                    return kSucc;
                }
            ptr = ind->next;
        }
        return kNotFound;
    }

    void DataIndexer::syncIndex() {
        msync(locs, INDEX_MAPSZ, MS_ASYNC);
        msync(keys, keyFileLen, MS_ASYNC);
    }
}