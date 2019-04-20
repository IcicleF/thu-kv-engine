// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"

namespace polar_race {

RetCode Engine::Open(const std::string& name, Engine** eptr) {
    return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

/*
 * Complete the functions below to implement you own engine
 */

char keybuf[MAX_KEYSZ + 5], valbuf[MAX_VALSZ + 5];

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
    *eptr = NULL;
    EngineRace *engine_race = new EngineRace(name);

    engine_race->logger.init();
    engine_race->indexer.init();
    engine_race->store.init();

    int keyLen, valLen;
    bool r = engine_race->logger.readLog(keybuf, &keyLen, valbuf, &valLen);
    if (r) {
        RetCode ret;
        if ((ret = engine_race->doWrite(PolarString(keybuf, keyLen), PolarString(valbuf, valLen), false)) != kSucc)
            return ret;
    }
    engine_race->logger.clearLog();

    *eptr = engine_race;
    return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
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
    }
unlock:
    pthread_mutex_unlock(&mu);
    return ret;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
    RetCode ret = kSucc;
    pthread_mutex_lock(&mu); {
        Location loc;
        if ((ret = indexer.find(key.data(), key.size(), &loc)) != kSucc)
            goto unlock;
        int valLen;
        if ((ret = store.readData(valbuf, &valLen, loc)) != kSucc)
            goto unlock;
        valbuf[valLen] = 0;
        *value = std::string(valbuf);
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
