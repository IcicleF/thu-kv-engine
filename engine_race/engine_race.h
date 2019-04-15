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
            ensureDirectory(pathJoin(dir, "index").c_str());
            ensureDirectory(pathJoin(dir, "data").c_str());
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
        pthread_mutex_t mu;
        DataIndexer indexer;
        DataStore store;
        DataLogger logger;
};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
