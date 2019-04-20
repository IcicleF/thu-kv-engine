#if !defined(ENGINE_RACE_DATA_LOGGER_H)
#define ENGINE_RACE_DATA_LOGGER_H

#include "utils.h"
#include <vector>
#include <functional>

#define LOG_START       0xABABABAB
#define LOG_END         0xEDEDEDED

#define LOG_FILESZ      134254952
#define LOG_LIM         130059248
#define LOG_MAX         256

/*
 * Structure of a valid log:
 *  - LOG_START         (4 bytes)
 *  - keyLen            (4 bytes)
 *  - valLen            (4 bytes)
 *  - key
 *  - val
 *  - LOG_END           (4 bytes)
 */

namespace polar_race {
    class DataLogger {
        public:
            explicit DataLogger(const std::string& root) : dir(root) { }
            ~DataLogger() {
                munmap(ptr, LOG_FILESZ);
                lockf(fd, F_ULOCK, 0);
                close(fd);
            }

            void init();
            
            void clearLog();
            void writeLog(const char *key, int keyLen, const char *value, int valLen);
            void readLog(std::vector<RecoveredLog> *logs);
            bool needFlush();

        private:
            std::string dir;
            void *ptr;
            char *fp, *fend;
            int fd;
            int cnt;
    };
} // namespace polar_race


#endif // ENGINE_RACE_DATA_LOGGER_H
