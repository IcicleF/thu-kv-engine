#include "data_logger.h"

using std::string;

namespace polar_race {
    void DataLogger::init() {
        std::string logFile = pathJoin(dir, "log");
        int fd = open(logFile.c_str(), O_RDWR | O_CREAT, 0777);
        if (fd < 0)
            exit(-1);
        int r = lockf(this->fd = fd, F_LOCK, 0);
        if (r < 0)
            exit(-1);
    }

    void DataLogger::clearLog() {
        int r = ftruncate(fd, 0);
        if (r < 0)
            exit(-1);
        lseek(fd, 0, SEEK_SET);
    }
    
    void DataLogger::log(const string& filename, uint32_t offset, uint32_t len, bool omit, void *oldData, void *newData) {
        char *buf = new char[6 * sizeof(uint32_t) + filename.size() + 2 * len + 5];
        volatile uint32_t logInt;
        uint32_t buflen = 0;

        logInt = LOG_START;
        bufWrite(buf, buflen, &logInt, sizeof(uint32_t));

        logInt = filename.size();
        bufWrite(buf, buflen, &logInt, sizeof(uint32_t));
        bufWrite(buf, buflen, filename.c_str(), logInt);

        logInt = offset;
        bufWrite(buf, buflen, &logInt, sizeof(uint32_t));
        logInt = len;
        bufWrite(buf, buflen, &logInt, sizeof(uint32_t));
        logInt = omit ? LOG_CTRL : 0;
        bufWrite(buf, buflen, &logInt, sizeof(uint32_t));

        if (!omit)
            bufWrite(buf, buflen, oldData, len);
        bufWrite(buf, buflen, newData, len);

        logInt = LOG_END;
        bufWrite(buf, buflen, &logInt, sizeof(uint32_t));
        
        write(fd, (void *)buf, buflen);
        fsync(fd);

        delete[] buf;
    }

    void DataLogger::commit() {
        volatile uint32_t logInt = LOG_COMMIT;
        write(fd, (void *)(&logInt), sizeof(uint32_t));
        fsync(fd);
    }

    int DataLogger::readLog(std::function<int(const std::string&, uint32_t, uint32_t, void *)> callback) {
        off_t bytes = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);
        
        if (bytes == 0)
            return 0;

        void *ptr = mmap(NULL, bytes, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED)
            return -1;
        uint32_t *commitFlag = (uint32_t *)(ptr + bytes - 4);
        if (*commitFlag != LOG_COMMIT)
            return -1;
        
        std::vector<TempLog> logs;
        void *end = ptr + bytes;
        while (ptr < end) { 
            uint32_t startFlag = bufRead(ptr, uint32_t);
            if (startFlag == LOG_COMMIT)
                break;
            if (startFlag != LOG_START)
                return -1;
            
            TempLog log;
            log.filenamelen = bufRead(ptr, uint32_t);
            log.filename = std::string((char *)ptr, log.filenamelen);
            ptr += log.filenamelen;
            
            log.offset = bufRead(ptr, uint32_t);
            log.len = bufRead(ptr, uint32_t);
            
            uint32_t logCtrl = bufRead(ptr, uint32_t);
            if (logCtrl == 0) {
                log.omit = false;
                log.oldData = ptr;
                ptr += log.len;
            }
            else if (logCtrl == LOG_CTRL) {
                log.omit = true;
                log.oldData = NULL;
            }
            else
                return -1;
            log.newData = ptr;
            ptr += log.len;
            
            if (bufRead(ptr, uint32_t) != LOG_END)
                return -1;
            logs.push_back(log);
        }

        if (ptr < end)
            return -1;
        for (auto log : logs)
            callback(log.filename, log.offset, log.len, log.newData);
        
        munmap(ptr, bytes);
        return 0;
    }
}