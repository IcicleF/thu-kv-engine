#include "data_logger.h"

using std::string;

namespace polar_race {
    extern bool needUpdate;

    void DataLogger::init() {
        std::string logFile = pathJoin(dir, "log");

        int fd = open(logFile.c_str(), O_RDWR | O_CREAT | O_DIRECT | O_SYNC, 0777);
        if (fd < 0)
            exit(-1);
        lockf(this->fd = fd, F_LOCK, 0);
        posix_fallocate(fd, 0, LOG_FILESZ);
        ptr = mmap(NULL, LOG_FILESZ, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        fend = (fp = (char *)ptr) + LOG_LIM;
    }

    void DataLogger::clearLog() {
        fp = (char *)ptr;
        *fp = 0;
        cnt = 0;
    }

    void DataLogger::writeLog(const char *key, int keyLen, const char *value, int valLen) {
        int offs = 0;
        uint32_t flag = LOG_START;
        bufWrite(fp, offs, &flag, sizeof(uint32_t));
        bufWrite(fp, offs, &keyLen, sizeof(int));
        bufWrite(fp, offs, &valLen, sizeof(int));
        bufWrite(fp, offs, key, keyLen);
        bufWrite(fp, offs, value, valLen);
        flag = LOG_END;
        bufWrite(fp, offs, &flag, sizeof(uint32_t));
        fp += offs;
        *fp = 0;
        ++cnt;
        if (cnt == LOG_MAX || fp >= fend)
            needUpdate = true;
    }

    void DataLogger::readLog(std::vector<RecoveredLog> *logs) {
        fp = (char *)ptr;
        logs->clear();
        while (fp < fend && *fp != 0) {
            if (*((uint32_t *)fp) != LOG_START)
                break;
            int keyLen = *((int *)fp + 1);
            int valLen = *((int *)fp + 2);
            fp += 12;
            if (*((uint32_t *)(fp + keyLen + valLen)) != LOG_END)
                break;
            logs->push_back(RecoveredLog(fp, keyLen, fp + keyLen, valLen));
            fp += keyLen + valLen + sizeof(uint32_t);
        }
    }

    bool DataLogger::needFlush() {
        return cnt > 0;
    }
}