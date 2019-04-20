#include "data_store.h"

namespace polar_race {
    void DataStore::init() {
        lastNo = -1;
        std::vector<std::string> files;
        getDirFiles(dir, &files);
        for (auto s : files) {
            int32_t no = std::stoi(s);
            if (no > lastNo)
                lastNo = no;
        }
        ++lastNo;

        std::string curFileName = pathJoin(dir, std::to_string(lastNo));
        fd = open(curFileName.c_str(), O_RDWR | O_CREAT, 0777);
        posix_fallocate(fd, 0, STORE_FILESZ);
        ptr = mmap(NULL, STORE_FILESZ, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        fend = (fp = flast = (char *)ptr) + STORE_FILESZ;
        close(fd);
    }
    
    RetCode DataStore::readData(char **value, int *valLen, Location loc) {
        if (loc.fileNo != lastNo) {
            int _fd = open(pathJoin(dir, std::to_string(loc.fileNo)).c_str(), O_RDONLY, 0777);
            if (_fd < 0)
                return kIOError;
            lseek(_fd, loc.offset, SEEK_SET);
            int datints[3];
            read(_fd, (void *)datints, 2 * sizeof(int));
            lseek(_fd, datints[0], SEEK_CUR);               // skip key
            read(_fd, *value, (*valLen = datints[1]));
            close(_fd);
        }
        else {
            char *vp = (char *)ptr + loc.offset;
            int keyLen = *((int *)vp);
            *valLen = *((int *)vp + 1);
            *value = vp + 2 * sizeof(int) + keyLen;
        }
        return kSucc;
    }

    RetCode DataStore::writeData(const char *key, int keyLen, const char *value, int valLen) {
        if (fp + 2 * sizeof(int) + keyLen + valLen >= fend) {
            munmap(ptr, STORE_FILESZ);

            std::string curFileName = pathJoin(dir, std::to_string(++lastNo));
            fd = open(curFileName.c_str(), O_RDWR | O_CREAT, 0777);
            posix_fallocate(fd, 0, STORE_FILESZ);
            ptr = mmap(NULL, STORE_FILESZ, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            fend = (fp = flast = (char *)ptr) + STORE_FILESZ;
            close(fd);
        }
        int offs = 0;
        bufWrite(fp, offs, &keyLen, sizeof(int));
        bufWrite(fp, offs, &valLen, sizeof(int));
        bufWrite(fp, offs, key, keyLen);
        bufWrite(fp, offs, value, valLen);
        fp += offs;
        
        return kSucc;
    }

    void DataStore::syncData() {
        if (fp == flast)
            return;
        msync(flast, fp - flast, MS_ASYNC);
        flast = fp;
    }
    
    void DataStore::getWriteLocation(Location *loc) {
        loc->fileNo = lastNo;
        loc->offset = fp - (char *)ptr;
    }
}