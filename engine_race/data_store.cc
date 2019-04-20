#include "data_store.h"

namespace polar_race {
    void DataStore::init() {
        lastNo = 0;
        std::vector<std::string> files;
        getDirFiles(dir, &files);
        for (auto s : files) {
            int32_t no = std::stoi(s);
            if (no > lastNo)
                lastNo = no;
        }

        std::string curFileName = pathJoin(dir, std::to_string(lastNo));
        fd = open(curFileName.c_str(), O_RDWR | O_CREAT, 0777);
        cur = lseek(fd, 0, SEEK_END);
    }
    
    RetCode DataStore::readData(char *value, int *valLen, Location loc) {
        int _fd = open(pathJoin(dir, std::to_string(loc.fileNo)).c_str(), O_RDONLY, 0777);
        if (_fd < 0)
            return kIOError;
        lseek(_fd, loc.offset, SEEK_SET);
        int datints[3];
        read(_fd, (void *)datints, 2 * sizeof(int));
        lseek(_fd, datints[0], SEEK_CUR);               // skip key
        read(_fd, value, (*valLen = datints[1]));
        close(_fd);
        return kSucc;
    }

    char datbuf[MAX_KEYSZ * 2 + MAX_VALSZ];
    RetCode DataStore::writeData(const char *key, int keyLen, const char *value, int valLen) {
        int recLen = 2 * sizeof(int) + keyLen + valLen;
        int offs = 0;
        bufWrite(datbuf, offs, &keyLen, sizeof(int));
        bufWrite(datbuf, offs, &valLen, sizeof(int));
        bufWrite(datbuf, offs, key, keyLen);
        bufWrite(datbuf, offs, value, valLen);

        if (write(fd, datbuf, recLen) < 0)
            return kIOError;
        fsync(fd);
        cur += recLen;
        if (cur >= STORE_FILESZ) {
            close(fd);
            std::string curFileName = pathJoin(dir, std::to_string(++lastNo));
            fd = open(curFileName.c_str(), O_RDWR | O_CREAT, 0777);
            cur = 0;
        }
        return kSucc;
    }
    
    void DataStore::getWriteLocation(Location *loc) {
        loc->fileNo = lastNo;
        loc->offset = cur;
    }
}