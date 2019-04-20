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
        if (ptr == MAP_FAILED) {
            close(fd);
            return kIOError;
        }
        locs = reinterpret_cast<off_t *>(ptr);
        if (new_create) {
            memset(locs, -1, INDEX_MAPSZ);
            msync(ptr, INDEX_MAPSZ, MS_SYNC);
        }

        std::string keyFile = pathJoin(dir, "keys");
        int key_fd = open(keyFile.c_str(), O_RDWR | O_CREAT, 0777);
        if (key_fd < 0) {
            close(fd);
            return kIOError;
        }
        this->key_fd = key_fd;
        return kSucc;
    }
    
    RetCode DataIndexer::insert(const char *key, int keyLen, Location loc) {
        static char buf[MAX_KEYSZ + 5];
        uint32_t index = strHash(key, keyLen);
        off_t ptr = locs[index], prev = -1;
        IndexItem ind;
        while (ptr != -1) {
            lseek(key_fd, ptr, SEEK_SET);
            if (read(key_fd, &ind, sizeof(IndexItem)) < 0)
                return kIOError;
            if (keyLen == ind.keyLen) {
                if (read(key_fd, buf, ind.keyLen) < 0)
                    return kIOError;
                if (memcpy(buf, key, keyLen) == 0)
                    break;
            }
            prev = ptr;
            ptr = ind.next;
        }
        if (prev != -1 && ptr != -1) {
            IndexItem prevInd;
            lseek(key_fd, prev, SEEK_SET);
            read(key_fd, &prevInd, sizeof(IndexItem));
            prevInd.next = ind.next;
            lseek(key_fd, -sizeof(IndexItem), SEEK_CUR);
            write(key_fd, &prevInd, sizeof(IndexItem));
        }

        ind.keyLen = keyLen;
        ind.fileNo = loc.fileNo;
        ind.offset = loc.offset;
        ind.next = locs[index];

        locs[index] = lseek(key_fd, 0, SEEK_END);
        msync(locs + index, sizeof(off_t), MS_SYNC);

        write(key_fd, &ind, sizeof(IndexItem));
        write(key_fd, key, keyLen);
        fsync(key_fd);

        return kSucc;
    }

    RetCode DataIndexer::find(const char *key, int keyLen, Location *loc) {
        static char buf[MAX_KEYSZ + 5];
        uint32_t index = strHash(key, keyLen);
        off_t ptr = locs[index];
        IndexItem ind;
        while (ptr != -1) {
            lseek(key_fd, ptr, SEEK_SET);
            if (read(key_fd, &ind, sizeof(IndexItem)) < 0)
                return kIOError;
            if (keyLen == ind.keyLen) {
                if (read(key_fd, buf, ind.keyLen) < 0)
                    return kIOError;
                if (memcmp(buf, key, keyLen) == 0) {
                    loc->fileNo = ind.fileNo;
                    loc->offset = ind.offset;
                    return kSucc;
                }
            }
            ptr = ind.next;
        }
        return kNotFound;
    }
}