#if !defined(ENGINE_RACE_DATA_LOGGER_H)
#define ENGINE_RACE_DATA_LOGGER_H

#include "utils.h"
#include <vector>
#include <functional>

#define LOG_START   0xABABABAB
#define LOG_END     0xEDEDEDED
#define LOG_CTRL    0x5555AAAA
#define LOG_COMMIT  0xF1E2D3C4

#define bufWrite(buf, offs, src, len) do { memcpy(buf + offs, (void *)(src), len); offs += len; } while(0)
#define bufRead(ptr, type) (([](void *&p) -> type { p += sizeof(type); return *((type *)(p - sizeof(type))); })(ptr))

/*
 * Structure of a valid log:
 *  - LOG_START                 (4 bytes)
 *  - Length of file name       (4 bytes)
 *  - File name
 *  - Offset                    (4 bytes)
 *  - Len                       (4 bytes)
 *  - Origin Omission Flag      (4 bytes)
 *  - Origin Data               ([Len] bytes if exist)
 *  - New Data                  ([Len] bytes)
 *  - LOG_END                   (4 bytes)
 * 
 * Structure of a valid log file:
 *  - logs
 *  - LOG_COMMIT                (4 bytes)
 */

namespace polar_race {
    class DataLogger {
        public:
            explicit DataLogger(const std::string& root) : dir(root) {
                std::string logFile = pathJoin(dir, "log");
                int fd = open(logFile.c_str(), O_RDWR | O_CREAT, 0777);
                if (fd < 0)
                    exit(-1);
                lockf(this->fd = fd, F_LOCK, 0);
            }
            ~DataLogger() {
                lockf(fd, F_ULOCK, 0);
            }

            void clearLog() {
                ftruncate(fd, 0);
                lseek(fd, 0, SEEK_SET);
            }
            void log(const std::string& filename, uint32_t offset, uint32_t len, bool omit, void *oldData, void *newData) {
                void *buf = (void *)(new char[6 * sizeof(uint32_t) + filename.size() + 2 * len + 5]);
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
                
                write(fd, buf, buflen);
                fsync(fd);

                delete[] buf;
            }
            void commit() {
                volatile uint32_t logInt = LOG_COMMIT;
                write(fd, (void *)(&logInt), sizeof(uint32_t));
                fsync(fd);
            }

            int readLog(std::function<int(const std::string&, uint32_t, uint32_t, bool, void *, void *)> callback) {
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
                    if (bufRead(ptr, uint32_t) == LOG_COMMIT)
                        return 0;
                    if (bufRead(ptr, uint32_t) != LOG_START)
                        return -1;
                    
                    TempLog log;
                    log.filenamelen = bufRead(ptr, uint32_t);
                    log.filename = std::string(ptr, log.filenamelen);
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
                for (int i = 0; i < logs.size(); ++i)
                    callback(logs[i].filename, logs[i].offset, logs[i].len, logs[i].omit, logs[i].oldData, logs[i].newData);
                
                munmap(ptr, bytes);
            }

        private:
            std::string dir;
            int fd;

            struct TempLog {
                uint32_t filenamelen;
                std::string filename;
                uint32_t offset;
                uint32_t len;
                bool omit;
                void *oldData;
                void *newData;
            };
    };
} // namespace polar_race


#endif // ENGINE_RACE_DATA_LOGGER_H
