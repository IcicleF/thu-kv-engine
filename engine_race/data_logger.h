#if !defined(ENGINE_RACE_DATA_LOGGER_H)
#define ENGINE_RACE_DATA_LOGGER_H

#include "utils.h"
#include <vector>
#include <functional>

#define LOG_START   0xABABABAB
#define LOG_END     0xEDEDEDED
#define LOG_CTRL    0x5555AAAA
#define LOG_COMMIT  0xF1E2D3C4

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
            explicit DataLogger(const std::string& root) : dir(root) { }
            ~DataLogger() {
                int r = lockf(fd, F_ULOCK, 0);
                if (r < 0)
                    exit(-1);
            }

            void init();

            void clearLog();
            void log(const std::string& filename, uint32_t offset, uint32_t len, bool omit, void *oldData, void *newData);
            void commit();

            int readLog(std::function<int(const std::string&, uint32_t, uint32_t, void *)> callback);

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
