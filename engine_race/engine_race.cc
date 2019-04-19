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

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
    *eptr = NULL;
    EngineRace *engine_race = new EngineRace(name);

    engine_race->logger.init();
    engine_race->store.init();

    engine_race->logger.readLog(writeToFile);
    engine_race->logger.clearLog();

    *eptr = engine_race;
    return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
}

char recbuf[MAX_VALSZ << 1];

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
    RetCode ret = kSucc;
    pthread_mutex_lock(&mu); {
        Location loc, next;

        // logger.clearLog();
        uint32_t keyHash = strHash(key.data(), key.size());
        if ((ret = indexer.getLocation(keyHash, &next)) != kSucc)
            goto unlock;
        if ((ret = store.getWriteLocation(key.size(), value.size(), &loc)) != kSucc)
            goto unlock;
        
        // todo: garbage removal
        uint32_t fileNo = getFileIndex(keyHash), fileOffs = getOffsetIndex(keyHash) * sizeof(Location);
        std::string indexFile = pathJoin(root, "index/" + std::to_string(fileNo));
        std::string dataFile = pathJoin(root, "data/" + std::to_string(loc.fileNo));
        logger.log(indexFile, fileOffs, sizeof(Location), false, (void *)&next, (void *)&loc);
        
        RecordMeta *meta = (RecordMeta *)recbuf;
        meta->keyLen = key.size();
        meta->valLen = value.size();
        meta->nextFileNo = next.fileNo;
        meta->nextOffset = next.offset;
        memcpy(recbuf + sizeof(RecordMeta), key.data(), key.size());
        memcpy(recbuf + sizeof(RecordMeta) + key.size(), value.data(), value.size());
        logger.log(dataFile, loc.offset, loc.len, true, NULL, (void *)recbuf);
        logger.commit();

        if (logger.readLog(writeToFile) < 0)
            exit(-1);
        logger.clearLog();
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
        uint32_t keyHash = strHash(key.data(), key.size());
        if ((ret = indexer.getLocation(keyHash, &loc)) != kSucc)
            goto unlock;
        if ((ret = store.readData(key.data(), key.size(), recbuf, &loc)) != kSucc)
            goto unlock;
        *value = std::string(recbuf);
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
