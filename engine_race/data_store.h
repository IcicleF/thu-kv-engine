#if !defined(ENGINE_RACE_DATA_STORE_H)
#define ENGINE_RACE_DATA_STORE_H

#include "utils.h"

namespace polar_race {
    class DataStore {
        public:
            explicit DataStore(const std::string& root) : dir(pathJoin(root, "data")) { }
            ~DataStore() { }

            RetCode read(const std::string& key, std::string& value, Location *loc) {
                
                return kSucc;
            }
            RetCode write(const std::string& key, const std::string &value, Location *loc) {
                
                return kSucc;
            }

        private:
            std::string dir;
    };
}

#endif // ENGINE_RACE_DATA_STORE_H
