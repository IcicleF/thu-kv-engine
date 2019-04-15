#if !defined(ENGINE_RACE_DATA_LOGGER_H)
#define ENGINE_RACE_DATA_LOGGER_H

#include "utils.h"

#define LOG_START   0xABABABAB
#define LOG_END     0xEDEDEDED
#define LOG_CTRL    0x5555AAAA
#define LOG_COMMIT  0x1F2E3D4C

#define bufWrite(buf, offs, src, len) do { memcpy(buf + offs, (void *)(src), len); offs += len; } while(0)

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
                
                write(fd, buf, buflen);
                fsync(fd);
                
                delete[] buf;
            }
            void commit() {
                volatile uint32_t logInt = LOG_COMMIT;
                write(fd, (void *)(&logInt), sizeof(uint32_t));
                fsync(fd);
            }

        private:
            std::string dir;
            int fd;
    };
} // namespace polar_race


#endif // ENGINE_RACE_DATA_LOGGER_H
