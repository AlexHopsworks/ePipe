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

struct FileProvenancePK {
  Int64 mInodeId;
  std::string mOperation;
  int mLogicalTime;
  Int64 mTimestamp;
  std::string mAppId;
  int mUserId;
  
  FileProvenancePK(Int64 inodeId, std::string operation, int logicalTime, Int64 timestamp, std::string appId, int userId) {
    mInodeId = inodeId;
    mOperation = operation;
    mLogicalTime = logicalTime;
    mTimestamp = timestamp;
    mAppId = appId;
    mUserId = userId;
  }

  std::string to_string() {
    std::stringstream out;
    out << mInodeId << "-" << mOperation << "-" << mLogicalTime << "-" << mTimestamp << "-" << mAppId << "-" << mUserId;
    return out.str();
  }
};

struct FileProvenanceRow {
  Int64 mInodeId;
  std::string mOperation;
  int mLogicalTime;
  Int64 mTimestamp;
  std::string mAppId;
  int mUserId;
  
  Int64 mPartitionId;
  Int64 mProjectId;
  Int64 mDatasetId;
  Int64 mParentId;
  std::string mInodeName;
  std::string mProjectName;
  std::string mDatasetName;
  std::string mP1Name;
  std::string mP2Name;
  std::string mParentName;
  std::string mUserName;
  std::string mXAttrName;
  int mLogicalTimeBatch;
  Int64 mTimestampBatch;
  int mDatasetLogicalTime;
  
  ptime mEventCreationTime;

  FileProvenancePK getPK() {
    return FileProvenancePK(mInodeId, mOperation, mLogicalTime, mTimestamp, mAppId, mUserId);
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
    
    stream << "PartitionId = " << mPartitionId << std::endl;
    stream << "ProjectId = " << mProjectId << std::endl;
    stream << "DatasetId = " << mDatasetId << std::endl;
    stream << "ParentId = " << mParentId << std::endl;
    stream << "InodeName = " << mInodeName << std::endl;
    stream << "ProjectName = " << mProjectName << std::endl;
    stream << "DatasetName = " << mDatasetName << std::endl;
    stream << "Parent1Name = " << mP1Name << std::endl;
    stream << "Parent2Name = " << mP2Name << std::endl;
    stream << "ParentName = " << mParentName << std::endl;
    stream << "UserName = " << mUserName << std::endl;
    stream << "XAttrName = " << mXAttrName << std::endl;
    stream << "LogicalTimeBatch = " << mLogicalTimeBatch << std::endl;
    stream << "TimestampBatch = " << mTimestampBatch << std::endl;
    stream << "DatasetLogicalTime = " << mDatasetLogicalTime << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }
};
  
struct FileProvenanceRowEqual {

  bool operator()(const FileProvenanceRow &lhs, const FileProvenanceRow &rhs) const {
    return lhs.mInodeId == rhs.mInodeId && lhs.mUserId == rhs.mUserId
            && lhs.mAppId == rhs.mAppId && lhs.mLogicalTime == rhs.mLogicalTime;
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
    
    return seed;
  }
};

struct FileProvenanceRowComparator {

  bool operator()(const FileProvenanceRow &r1, const FileProvenanceRow &r2) const {
    if (r1.mInodeId == r2.mInodeId) {
      return r1.mLogicalTime > r2.mLogicalTime;
    } else {
      return r1.mInodeId > r2.mInodeId;
    }
  }
};

typedef ConcurrentQueue<FileProvenanceRow> CPRq;
typedef boost::heap::priority_queue<FileProvenanceRow, boost::heap::compare<FileProvenanceRowComparator> > PRpq;
typedef std::vector <boost::optional<FileProvenancePK> > PKeys;
typedef std::vector <FileProvenanceRow> Pq;

typedef std::vector <FileProvenanceRow> Pv;
typedef boost::unordered_map<Uint64, Pv* > ProvenanceRowsByGCI;
typedef boost::tuple<std::vector<Uint64>*, ProvenanceRowsByGCI* > ProvenanceRowsGCITuple;

class FileProvenanceLogTable : public DBWatchTable<FileProvenanceRow> {
public:

  FileProvenanceLogTable() : DBWatchTable("hdfs_file_provenance_log") {
    addColumn("inode_id");
    addColumn("inode_operation");
    addColumn("io_logical_time");
    addColumn("io_timestamp");
    addColumn("io_app_id");
    addColumn("io_user_id");
    addColumn("i_partition_id");
    addColumn("project_i_id");
    addColumn("dataset_i_id");
    addColumn("parent_i_id");
    addColumn("i_name");
    addColumn("project_name");
    addColumn("dataset_name");
    addColumn("i_p1_name");
    addColumn("i_p2_name");
    addColumn("i_parent_name");
    addColumn("io_user_name");
    addColumn("i_xattr_name");
    addColumn("io_logical_time_batch");
    addColumn("io_timestamp_batch");
    addColumn("ds_logical_time");
    addWatchEvent(NdbDictionary::Event::TE_INSERT);
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
    row.mPartitionId = value[6]->int64_value();
    row.mProjectId = value[7]->int64_value();
    row.mDatasetId = value[8]->int64_value();
    row.mParentId = value[9]->int64_value();
    row.mInodeName = get_string(value[10]);
    row.mProjectName = get_string(value[11]);
    row.mDatasetName = get_string(value[12]);
    row.mP1Name = get_string(value[13]);
    row.mP2Name = get_string(value[14]);
    row.mParentName = get_string(value[15]);
    row.mUserName = get_string(value[16]);
    row.mXAttrName = get_string(value[17]);
    row.mLogicalTimeBatch = value[18]->int32_value();
    row.mTimestampBatch = value[19]->int64_value();
    row.mDatasetLogicalTime = value[20]->int32_value();
    return row;
  }

  void removeLogs(Ndb* connection, PKeys& pks) {
    start(connection);
    for (PKeys::iterator it = pks.begin(); it != pks.end(); ++it) {

      boost::optional<FileProvenancePK> pk = *it;
      if(pk) {
        AnyMap a;
        a[0] = pk.get().mInodeId;
        a[1] = pk.get().mOperation;
        a[2] = pk.get().mLogicalTime;
        a[3] = pk.get().mTimestamp;
        a[4] = pk.get().mAppId;
        a[5] = pk.get().mUserId;
        
        doDelete(a);
        LOG_DEBUG("Delete file provenance row: " << pk.get().to_string());
      }
    }
    end();
  }

  std::string getPKStr(FileProvenanceRow row) {
    return row.getPK().to_string();
  }
};
#endif /* FILEPROVENANCELOGTABLE_H */

