#if !defined(ENGINE_RACE_DATA_LOGGER_H)
#define ENGINE_RACE_DATA_LOGGER_H

#include "utils.h"
#include <vector>
#include <functional>

#define LOG_START       0xABABABAB
#define LOG_END         0xEDEDEDED

#define CHECKPOINT_SZ   (1 << 25)

/*
 * Structure of a valid log:
 *  - LOG_START         (4 bytes)
 *  - keyLen            (4 bytes)
 *  - valLen            (4 bytes)
 *  - key
 *  - val
 *  - pointer to head   (4 bytes)
 *  - LOG_END           (4 bytes)
 */

namespace polar_race {
    class DataLogger {
        public:
            explicit DataLogger(const std::string& root) : dir(root), cur(0) { }
            ~DataLogger() {
                int r = lockf(fd, F_ULOCK, 0);
                if (r < 0)
                    exit(-1);
            }

            void init();
            
            void clearLog();
            void writeLog(const char *key, int keyLen, const char *value, int valLen);
            bool readLog(char *key, int *keyLen, char *value, int *valLen);

        private:
            std::string dir;
            int fd;
            int cur;
    };
} // namespace polar_race


#endif // ENGINE_RACE_DATA_LOGGER_H
