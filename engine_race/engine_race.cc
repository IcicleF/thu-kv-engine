// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"

namespace polar_race {

bool needUpdate;

RetCode Engine::Open(const std::string& name, Engine** eptr) {
    return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

/*
 * Complete the functions below to implement you own engine
 */

char keybuf[MAX_KEYSZ + 5], valbuf[MAX_VALSZ + 5];
std::vector<RecoveredLog> logs;

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
    *eptr = NULL;
    EngineRace *engine_race = new EngineRace(name);

    engine_race->logger.init();
    engine_race->indexer.init();
    engine_race->store.init();

    needUpdate = false;

    int keyLen, valLen;
    engine_race->logger.readLog(&logs);
    if (logs.size() > 0) {
        for (std::size_t i = 0; i < logs.size(); ++i)
            engine_race->doWrite(PolarString(logs[i].key, logs[i].keyLen), PolarString(logs[i].value, logs[i].valLen), false);
        engine_race->indexer.syncIndex();
        engine_race->store.syncData();
    }
    engine_race->logger.clearLog();

    *eptr = engine_race;
    return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
    indexer.syncIndex();
    store.syncData();
    logger.clearLog();
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
    return doWrite(key, value, true);
}

RetCode EngineRace::doWrite(const PolarString& key, const PolarString& value, bool doLog) {
    RetCode ret = kSucc;
    pthread_mutex_lock(&mu); {
        if (doLog)
            logger.writeLog(key.data(), key.size(), value.data(), value.size());
        Location loc;
        store.getWriteLocation(&loc);
        if ((ret = store.writeData(key.data(), key.size(), value.data(), value.size())) != kSucc)
            goto unlock;
        if ((ret = indexer.insert(key.data(), key.size(), loc)) != kSucc)
            goto unlock;
        if (needUpdate) {
            needUpdate = false;
            indexer.syncIndex();
            store.syncData();
            logger.clearLog();
        }
    }
unlock:
    pthread_mutex_unlock(&mu);
    return ret;
}

extern bool hajime;

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
    RetCode ret = kSucc;
    pthread_mutex_lock(&mu); {
        Location loc;
        if ((ret = indexer.find(key.data(), key.size(), &loc)) != kSucc)
            goto unlock;
        char *valptr = valbuf;
        int valLen;
        if ((ret = store.readData(&valptr, &valLen, loc)) != kSucc)
            goto unlock;
        *value = std::string(valptr, valLen);
    }
unlock:
    pthread_mutex_unlock(&mu);
    return ret;
}

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
    return kSucc;
}

}  // namespace polar_race
