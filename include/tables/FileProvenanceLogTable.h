/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * ePipe is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * ePipe is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef FILEPROVENANCELOGTABLE_H
#define FILEPROVENANCELOGTABLE_H
#include "DBWatchTable.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/document.h"

#include "ConcurrentPriorityQueue.h"
#include "ConcurrentQueue.h"
#include "XAttrTable.h"
#include "FileProvenanceXAttrBufferTable.h"
#include "FileProvenanceConstantsRaw.h"

struct FileProvenancePK {
  Int64 mInodeId;
  std::string mOperation;
  int mLogicalTime;
  Int64 mTimestamp;
  std::string mAppId;
  int mUserId;
  std::string mTieBreaker;

  FileProvenancePK() {
  }

  FileProvenancePK(Int64 inodeId, std::string operation, int logicalTime, Int64 timestamp, std::string appId,
      int userId, std::string tieBreaker) {
    mInodeId = inodeId;
    mOperation = operation;
    mLogicalTime = logicalTime;
    mTimestamp = timestamp;
    mAppId = appId;
    mUserId = userId;
    mTieBreaker = tieBreaker;
  }

  std::string to_string() const {
    std::stringstream out;
    out << mInodeId << "-" << mOperation << "-" << mLogicalTime << "-" << mTimestamp << "-" << mAppId << "-" << mUserId
    <<"-"<<mTieBreaker;
    return out.str();
  }

  AnyMap getMap() {
    AnyMap a;
    a[0] = mInodeId;
    a[1] = mOperation;
    a[2] = mLogicalTime;
    a[3] = mTimestamp;
    a[4] = mAppId;
    a[5] = mUserId;
    a[6] = mTieBreaker;
    return a;
  }
};

struct FileProvenanceRow {
  Int64 mInodeId;
  std::string mOperation;
  int mLogicalTime;
  Int64 mTimestamp;
  std::string mAppId;
  int mUserId;
  std::string mTieBreaker;
  
  Int64 mPartitionId;
  Int64 mProjectId;
  Int64 mDatasetId;
  Int64 mParentId;
  std::string mInodeName;
  std::string mProjectName;
  std::string mDatasetName;
  std::string mP1Name;
  Int64 mP1Id;
  std::string mP2Name;
  Int64 mP2Id;
  std::string mParentName;
  std::string mUserName;
  std::string mXAttrName;
  int mLogicalTimeBatch;
  Int64 mTimestampBatch;
  int mDatasetLogicalTime;
  Int16 mXAttrNumParts;

  ptime mEventCreationTime;

  FileProvenancePK getPK() {
    return FileProvenancePK(mInodeId, mOperation, mLogicalTime, mTimestamp, mAppId, mUserId, mTieBreaker);
  }

  FPXAttrBufferPK getXAttrBufferPK() {
    return FPXAttrBufferPK(mInodeId, FileProvenanceConstantsRaw::XATTRS_USER_NAMESPACE, mXAttrName, mLogicalTime, mXAttrNumParts);
  }

  std::string to_string() {
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "InodeId = " << mInodeId << std::endl;
    stream << "Operation = " << mOperation << std::endl;
    stream << "LogicalTime = " << mLogicalTime << std::endl;
    stream << "Timestamp = " << mTimestamp << std::endl;
    stream << "AppId = " << mAppId << std::endl;
    stream << "UserId = " << mUserId << std::endl;
    stream << "TieBreaker = " << mTieBreaker << std::endl;
    
    stream << "PartitionId = " << mPartitionId << std::endl;
    stream << "ProjectId = " << mProjectId << std::endl;
    stream << "DatasetId = " << mDatasetId << std::endl;
    stream << "ParentId = " << mParentId << std::endl;
    stream << "InodeName = " << mInodeName << std::endl;
    stream << "ProjectName = " << mProjectName << std::endl;
    stream << "DatasetName = " << mDatasetName << std::endl;
    stream << "Parent1Name = " << mP1Name << std::endl;
    stream << "Parent1Id = " << mP1Id << std::endl;
    stream << "Parent2Name = " << mP2Name << std::endl;
    stream << "Parent2Id = " << mP2Id << std::endl;
    stream << "ParentName = " << mParentName << std::endl;
    stream << "UserName = " << mUserName << std::endl;
    stream << "XAttrName = " << mXAttrName << std::endl;
    stream << "LogicalTimeBatch = " << mLogicalTimeBatch << std::endl;
    stream << "TimestampBatch = " << mTimestampBatch << std::endl;
    stream << "DatasetLogicalTime = " << mDatasetLogicalTime << std::endl;
    stream << "XAttrNumParts = " << mXAttrNumParts << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }
};
  
struct FileProvenanceRowEqual {

  bool operator()(const FileProvenanceRow &lhs, const FileProvenanceRow &rhs) const {
    return lhs.mInodeId == rhs.mInodeId && lhs.mUserId == rhs.mUserId
            && lhs.mAppId == rhs.mAppId && lhs.mLogicalTime == rhs.mLogicalTime
            && lhs.mTieBreaker == rhs.mTieBreaker;
  }
};

struct FileProvenanceRowHash {

  std::size_t operator()(const FileProvenanceRow &a) const {
    std::size_t seed = 0;
    boost::hash_combine(seed, a.mInodeId);
    boost::hash_combine(seed, a.mOperation);
    boost::hash_combine(seed, a.mLogicalTime);
    boost::hash_combine(seed, a.mTimestamp);
    boost::hash_combine(seed, a.mAppId);
    boost::hash_combine(seed, a.mUserId);
    boost::hash_combine(seed, a.mTieBreaker);
    
    return seed;
  }
};

struct FileProvenanceRowComparator {

  bool operator()(const FileProvenanceRow &r1, const FileProvenanceRow &r2) const {
    if (r1.mInodeId == r2.mInodeId) {
      return r1.mLogicalTime > r2.mLogicalTime;
    }
    if(r1.mDatasetId == r2.mDatasetId) {
      if(r1.mDatasetLogicalTime == r2.mDatasetLogicalTime) {
        //if both operations work on a dataset and have the same logical time
        //delete dataset should be last operation processed in epoch for this dataset logical time
        if(isDeleteDataset(r1)) {
          return true;
        }
        if(isDeleteDataset(r2)) {
          return false;
        }
        //dataset xattr attach should be first operation processed in epoch for this dataset logical time
        if(isDatasetProvCore(r1)) {
          return false;
        }
        if(isDatasetProvCore(r2)) {
          return true;
        }
      } else {
        return r1.mDatasetLogicalTime > r2.mDatasetLogicalTime;
      }
    }
    return r1.mInodeId > r2.mInodeId;
  }

  bool isDeleteDataset(const FileProvenanceRow &r) const {
    return r.mInodeId == r.mDatasetId
     && FileProvenanceConstantsRaw::findOp(r.mOperation) == FileProvenanceConstantsRaw::Operation::OP_DELETE;
  }
  bool isDatasetProvCore(const FileProvenanceRow &r) const {
    return r.mInodeId == r.mDatasetId
    && (FileProvenanceConstantsRaw::findOp(r.mOperation) == FileProvenanceConstantsRaw::Operation::OP_XATTR_ADD
    || FileProvenanceConstantsRaw::findOp(r.mOperation) == FileProvenanceConstantsRaw::Operation::OP_XATTR_UPDATE)
    && r.mXAttrName == FileProvenanceConstantsRaw::XATTR_PROV_CORE;
  }
};

typedef ConcurrentQueue<FileProvenanceRow> CPRq;
typedef boost::heap::priority_queue<FileProvenanceRow, boost::heap::compare<FileProvenanceRowComparator> > PRpq;
typedef std::vector <boost::optional<FileProvenancePK> > PKeys;
typedef std::vector <FileProvenanceRow> Pq;

class FProvCache {
public:
  /* we use a max timestamp shift of 1h
   * we cache the existance of a project for 1 hour before checking again if the project is still there
   */
  FProvCache(int lru_cap, const char* prefix) : maxTimestampShift(1000*3600), mProjects(lru_cap, prefix) {}

  bool projectExists(Int64 projectIId, Int64 timestamp) {
    /* yes we use operation timestamp - in case of recovery we might be doing pointless checks as the timestamps are obsolete
     * but they won't be that many and it simplifies logic
     */
    boost::optional<std::pair<std::string, Int64>> old_entry = mProjects.get(projectIId);
    if(old_entry) {
      if (timestamp <= old_entry.get().second + maxTimestampShift) {
        return true;
      } else {
        LOG_DEBUG("project exists - cached entry too old:" << old_entry.get().second << " op timestamp:" << timestamp);
        mProjects.remove(projectIId);
        return false;
      }
    } else {
      return false;
    }
  }

  void addProjectExists(Int64 projectIId, std::string projectName, Int64 timestamp) {
    mProjects.put(projectIId, std::make_pair(projectName, timestamp));
  }

  std::string getProjectName(Int64 projectIId) {
    return mProjects.get(projectIId).get().first;
  }

private:
  int maxTimestampShift;
  Cache<Int64, std::pair<std::string, Int64>> mProjects;
};

typedef CacheSingleton<FProvCache> FileProvCache;

class FileProvenanceLogTable : public DBWatchTable<FileProvenanceRow> {
public:
  struct FileProvLogHandler : public LogHandler{
    FileProvenancePK mPK;
    boost::optional<FPXAttrBufferPK> mBufferPK;

    FileProvLogHandler(FileProvenancePK pk, boost::optional<FPXAttrBufferPK> bufferPK) :
    mPK(pk), mBufferPK(bufferPK) {}

    void removeLog(Ndb* connection) const override {
      LOG_ERROR("do not use - logic error");
      std::stringstream cause;
      cause << "do not use - logic error";
      throw std::logic_error(cause.str());
    }
    LogType getType() const override {
      return LogType::PROVFILELOG;
    }

    std::string getDescription() const override {
      std::stringstream out;
      out << "FileProvLog (hdfs_file_provenance_log) Key (" << mPK.to_string() << ")";
      return out.str();
    }
  };

  FileProvenanceLogTable(int file_lru_cap, int xattr_lru_cap) : DBWatchTable("hdfs_file_provenance_log", new FileProvenanceXAttrBufferTable(xattr_lru_cap)) {
    addColumn("inode_id");
    addColumn("inode_operation");
    addColumn("io_logical_time");
    addColumn("io_timestamp");
    addColumn("io_app_id");
    addColumn("io_user_id");
    addColumn("tb");
    addColumn("i_partition_id");
    addColumn("project_i_id");
    addColumn("dataset_i_id");
    addColumn("parent_i_id");
    addColumn("i_name");
    addColumn("project_name");
    addColumn("dataset_name");
    addColumn("i_p1_name");
    addColumn("i_p1_id");
    addColumn("i_p2_name");
    addColumn("i_p2_id");
    addColumn("i_parent_name");
    addColumn("io_user_name");
    addColumn("i_xattr_name");
    addColumn("io_logical_time_batch");
    addColumn("io_timestamp_batch");
    addColumn("ds_logical_time");
    addColumn("i_xattr_num_parts");
    addWatchEvent(NdbDictionary::Event::TE_INSERT);
    FileProvCache::getInstance(file_lru_cap, "FileProv");
  }

  FileProvenanceRow getRow(NdbRecAttr* value[]) {
    FileProvenanceRow row;
    row.mEventCreationTime = Utils::getCurrentTime();
    row.mInodeId = value[0]->int64_value();
    row.mOperation = get_string(value[1]);
    row.mLogicalTime = value[2]->int32_value();
    row.mTimestamp = value[3]->int64_value();
    row.mAppId = get_string(value[4]);
    row.mUserId = value[5]->int32_value();
    row.mTieBreaker = get_string(value[6]);
    row.mPartitionId = value[7]->int64_value();
    row.mProjectId = value[8]->int64_value();
    row.mDatasetId = value[9]->int64_value();
    row.mParentId = value[10]->int64_value();
    row.mInodeName = get_string(value[11]);
    row.mProjectName = get_string(value[12]);
    row.mDatasetName = get_string(value[13]);
    row.mP1Name = get_string(value[14]);
    row.mP1Id = value[15]->int64_value();
    row.mP2Name = get_string(value[16]);
    row.mP2Id = value[17]->int64_value();
    row.mParentName = get_string(value[18]);
    row.mUserName = get_string(value[19]);
    row.mXAttrName = get_string(value[20]);
    row.mLogicalTimeBatch = value[21]->int32_value();
    row.mTimestampBatch = value[22]->int64_value();
    row.mDatasetLogicalTime = value[23]->int32_value();
    row.mXAttrNumParts = value[24]->short_value();
    return row;
  }

  void cleanLogs(Ndb* connection, std::vector<const LogHandler*>& logrh) {
    try{
      cleanLogsOneTransaction(connection, logrh);
    } catch (NdbTupleDidNotExist &e){
      cleanLogsMultiTransaction(connection, logrh);
    }
  }

  void cleanLog(Ndb* connection, const LogHandler* log) {
    if (log != nullptr && log->getType() == LogType::PROVFILELOG) {
      const FileProvLogHandler *fplog = static_cast<const FileProvLogHandler *>(log);
      try{
        start(connection);
        _doDeleteOnCompanion(fplog);
        _doDelete(fplog);
        end();
      } catch (NdbTupleDidNotExist &e){
        doDeleteOnCompanionTransaction(connection, fplog);
        doDeleteTransaction(connection, fplog);
      }
    }
  }

  std::string getPKStr(FileProvenanceRow row) {
    return row.getPK().to_string();
  }

  LogHandler* getLogHandler(FileProvenancePK pk, boost::optional<FPXAttrBufferPK> bufferPK) {
    return new FileProvLogHandler(pk, bufferPK);
  }

  boost::optional<FPXAttrBufferRow> getCompanionRow(Ndb* connection, FPXAttrBufferPK key) {
    FileProvenanceXAttrBufferTable* mXAttrBuffer = static_cast<FileProvenanceXAttrBufferTable*>(mCompanionTableBase);
    return mXAttrBuffer->get(connection, key);
  }

  std::map<int, boost::optional<FPXAttrBufferRow>> getProvCore(Ndb* connection, Int64 inodeId, int fromLogicalTime, int toLogicalTime) {
    FileProvenanceXAttrBufferTable* mXAttrBuffer = static_cast<FileProvenanceXAttrBufferTable*>(mCompanionTableBase);
    return mXAttrBuffer->getProvCore(connection, inodeId, fromLogicalTime, toLogicalTime);
  }

private:
  void cleanLogsOneTransaction(Ndb* connection, std::vector<const LogHandler*>&logrh) {
    start(connection);
    for (auto log : logrh) {
      if (log != nullptr && log->getType() == LogType::PROVFILELOG) {
        const FileProvLogHandler *fplog = static_cast<const FileProvLogHandler *>(log);
        _doDeleteOnCompanion(fplog);
        _doDelete(fplog);
      }
    }
    end();
  }

  void cleanLogsMultiTransaction(Ndb* connection, std::vector<const LogHandler*>&logrh) {
    for (auto log : logrh) {
      if (log != nullptr && log->getType() == LogType::PROVFILELOG) {
        const FileProvLogHandler *fplog = static_cast<const FileProvLogHandler *>(log);
        doDeleteOnCompanionTransaction(connection, fplog);
        doDeleteTransaction(connection, fplog);
      }
    }
  }

  void doDeleteOnCompanionTransaction(Ndb* connection, const FileProvLogHandler *fplog){
    try{
      start(connection);
      _doDeleteOnCompanion(fplog);
      end();
    } catch (NdbTupleDidNotExist &e){
      LOG_DEBUG("Companion Log row was already deleted");
    }
  }

  void doDeleteTransaction(Ndb* connection, const FileProvLogHandler *fplog){
    try{
      start(connection);
      _doDelete(fplog);
      end();
    } catch (NdbTupleDidNotExist &e){
      LOG_DEBUG("Log row was already deleted");
    }
  }

  void _doDeleteOnCompanion(const FileProvLogHandler *fplog){
    if (fplog->mBufferPK){
      FPXAttrBufferPK cPK = fplog->mBufferPK.get();
      AnyVec companionPKs = cPK.getKeysVec();
      LOG_DEBUG("Delete xattr buffer row: " << cPK.to_string());
      for(auto& pk : companionPKs){
        doDeleteOnCompanion(pk);
      }
    }
  }

  void _doDelete(const FileProvLogHandler *fplog){
    FileProvenancePK fPK = fplog->mPK;
    AnyMap fileProvPK = fPK.getMap();
    LOG_DEBUG("Delete file provenance row: " << fPK.to_string());
    doDelete(fileProvPK);
  }
};
#endif /* FILEPROVENANCELOGTABLE_H */

