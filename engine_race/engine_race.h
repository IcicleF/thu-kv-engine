// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_

#include <pthread.h>
#include <string>

#include "data_indexer.h"
#include "data_store.h"
#include "data_logger.h"

namespace polar_race {

class EngineRace : public Engine  {
    public:
        static RetCode Open(const std::string& name, Engine** eptr);
      
        explicit EngineRace(const std::string& dir)
            : mu(PTHREAD_MUTEX_INITIALIZER), indexer(dir), store(dir), logger(dir) {
            root = dir;
            ensureDirectory(dir);
            ensureDirectory(pathJoin(dir, "data"));
        }
      
        ~EngineRace();
      
        RetCode Write(const PolarString& key,
            const PolarString& value) override;
      
        RetCode Read(const PolarString& key,
            std::string* value) override;
      
        /*
         * NOTICE: Implement 'Range' in quarter-final,
         *         you can skip it in preliminary.
         */
        RetCode Range(const PolarString& lower,
            const PolarString& upper,
            Visitor &visitor) override;
    
    private:
        RetCode doWrite(const PolarString& key, const PolarString& value, bool doLog);
    
    private: 
        pthread_mutex_t mu;
        std::string root;
        DataIndexer indexer;
        DataStore store;
        DataLogger logger;
};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
