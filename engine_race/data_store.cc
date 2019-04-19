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
    }
    
    RetCode DataStore::readData(const char *key, int keyLen, char *value, Location *loc) {
        if (!findLocation(key, keyLen, loc)) 
            return kNotFound;
        std::string filename = pathJoin(dir, std::to_string(loc->fileNo));
        int fd = open(filename.c_str(), O_RDONLY, 0777);
        if (fd < 0)
            return kIOError;
        lseek(fd, loc->offset, SEEK_SET);

        RecordMeta rec;
        read(fd, (void *)(&rec), sizeof(RecordMeta));
        lseek(fd, rec.keyLen, SEEK_CUR);
        read(fd, (void *)value, rec.valLen);
        value[rec.valLen] = 0;
        close(fd);

        loc->len = rec.valLen;
        return kSucc;
    }
    
    RetCode DataStore::getWriteLocation(int keyLen, int valLen, Location *loc) {
        loc->len = sizeof(RecordMeta) + keyLen + valLen;
        off_t fileLen;

        if (lastNo >= 0) {
            int fd = open(pathJoin(dir, std::to_string(lastNo)).c_str(), O_RDWR, 0777);
            if (fd < 0)
                return kIOError;
            fileLen = lseek(fd, 0, SEEK_END);

            if (fileLen + loc->len > STORE_FILESZ) {
                close(fd);
                ++lastNo;
                std::string fileName = pathJoin(dir, std::to_string(lastNo));
                fd = open(fileName.c_str(), O_RDWR | O_CREAT, 0777);
                if (fd < 0)
                    return kIOError;
                fileLen = 0;
            }
            close(fd);
        }
        else {
            lastNo = 0;
            int fd = open(pathJoin(dir, "0").c_str(), O_RDWR | O_CREAT, 0777);
            if (fd < 0)
                return kIOError;
            fileLen = 0;
        }
        loc->fileNo = lastNo;
        loc->offset = fileLen;
        return kSucc;
    }

    char reckey[1 << 11];

    bool DataStore::findLocation(const char *key, int keyLen, Location *loc) {
        if (loc == NULL)
            return false;
        while (loc->fileNo != -1) {
            std::string filename = pathJoin(dir, std::to_string(loc->fileNo));
            int fd = open(filename.c_str(), O_RDONLY, 0777);
            if (fd < 0)
                return false;
            lseek(fd, loc->offset, SEEK_SET);

            RecordMeta rec;
            read(fd, (void *)(&rec), sizeof(RecordMeta));
            read(fd, (void *)reckey, rec.keyLen);
            close(fd);

            if ((uint32_t)keyLen == rec.keyLen && memcmp(reckey, key, rec.keyLen) == 0)
                return true;
            loc->fileNo = rec.nextFileNo;
            loc->offset = rec.nextOffset;
        }
        return false;
    }
}