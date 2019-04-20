#include "data_logger.h"

using std::string;

namespace polar_race {
    void DataLogger::init() {
        std::string logFile = pathJoin(dir, "log");

        int fd = open(logFile.c_str(), O_RDWR | O_CREAT | O_DIRECT | O_SYNC, 0777);
        if (fd < 0)
            exit(-1);
        int r = lockf(this->fd = fd, F_LOCK, 0);
        if (r < 0)
            exit(-1);
        cur = lseek(fd, 0, SEEK_END);
    }

    void DataLogger::clearLog() {
        int r = ftruncate(fd, 0);
        if (r < 0)
            exit(-1);
        lseek(fd, cur = 0, SEEK_SET);
    }

    char logbuf[MAX_KEYSZ * 2 + MAX_VALSZ];
    void DataLogger::writeLog(const char *key, int keyLen, const char *value, int valLen) {
        int recLen = 5 * sizeof(int) + keyLen + valLen;
        if (cur + recLen > CHECKPOINT_SZ)
            clearLog();
        int offs = 0;
        uint32_t flag = LOG_START;
        bufWrite(logbuf, offs, &flag, sizeof(uint32_t));
        bufWrite(logbuf, offs, &keyLen, sizeof(int));
        bufWrite(logbuf, offs, &valLen, sizeof(int));
        bufWrite(logbuf, offs, key, keyLen);
        bufWrite(logbuf, offs, value, valLen);
        bufWrite(logbuf, offs, &cur, sizeof(int));
        flag = LOG_END;
        bufWrite(logbuf, offs, &flag, sizeof(uint32_t));
        write(fd, logbuf, recLen);
        fsync(fd);
        cur += recLen;
    }

    bool DataLogger::readLog(char *key, int *keyLen, char *value, int *valLen) {
        if (cur < 2 * sizeof(uint32_t))
            return false;
        lseek(fd, -2 * sizeof(uint32_t), SEEK_END);
        uint32_t recints[5];
        read(fd, (void *)recints, 2 * sizeof(uint32_t));
        if (recints[1] == LOG_END) {
            lseek(fd, recints[0], SEEK_SET);
            read(fd, (void *)recints, 3 * sizeof(uint32_t));
            if (recints[0] != LOG_START)
                return false;
            *keyLen = recints[1];
            *valLen = recints[2];
            read(fd, key, *keyLen);
            read(fd, value, *valLen);
            return true;
        }
        return false;
    }
}