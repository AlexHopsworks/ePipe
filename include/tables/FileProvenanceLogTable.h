/*
 * Copyright (C) 2018 Hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
/* 
 * File:   FileProvenanceLogTable.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
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
  string mOperation;
  int mLogicalTime;
  Int64 mTimestamp;
  string mAppId;
  int mUserId;
  
  FileProvenancePK(Int64 inodeId, string operation, int logicalTime, Int64 timestamp, string appId, int userId) {
    mInodeId = inodeId;
    mOperation = operation;
    mLogicalTime = logicalTime;
    mTimestamp = timestamp;
    mAppId = appId;
    mUserId = userId;
  }

  string to_string() {
    stringstream out;
    out << mInodeId << "-" << mOperation << "-" << mLogicalTime << "-" << mTimestamp << "-" << mAppId << "-" << mUserId;
    return out.str();
  }
};

struct FileProvenanceRow {
  Int64 mInodeId;
  string mOperation;
  int mLogicalTime;
  Int64 mTimestamp;
  string mAppId;
  int mUserId;
  
  Int64 mPartitionId;
  Int64 mP1Id;
  Int64 mP2Id;
  Int64 mP3Id;
  Int64 mDatasetId;
  Int64 mProjectId;
  string mInodeName;
  string mDatasetName;
  int mLogicalTimeBatch;
  Int64 mTimestampBatch;
  
  ptime mEventCreationTime;

  FileProvenancePK getPK() {
    return FileProvenancePK(mInodeId, mOperation, mLogicalTime, mTimestamp, mAppId, mUserId);
  }

  string to_string() {
    stringstream stream;
    stream << "-------------------------" << endl;
    stream << "InodeId = " << mInodeId << endl;
    stream << "Operation = " << mOperation << endl;
    stream << "LogicalTime = " << mLogicalTime << endl;
    stream << "Timestamp = " << mTimestamp << endl;
    stream << "AppId = " << mAppId << endl;
    stream << "UserId = " << mUserId << endl;
    
    stream << "PartitionId = " << mPartitionId << endl;
    stream << "Parent1Id = " << mP1Id << endl;
    stream << "Parent2Id = " << mP2Id << endl;
    stream << "Parent3Id = " << mP3Id << endl;
    stream << "DatasetId = " << mDatasetId << endl;
    stream << "ProjectId = " << mProjectId << endl;
    stream << "InodeName = " << mInodeName << endl;
    stream << "DatasetName = " << mDatasetName << endl;
    stream << "LogicalTimeBatch = " << mLogicalTimeBatch << endl;
    stream << "TimestampBatch = " << mTimestampBatch << endl;
    stream << "-------------------------" << endl;
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
typedef vector<FileProvenancePK> PKeys;
typedef vector<FileProvenanceRow> Pq;

typedef vector<FileProvenanceRow> Pv;
typedef boost::unordered_map<Uint64, Pv* > ProvenanceRowsByGCI;
typedef boost::tuple<vector<Uint64>*, ProvenanceRowsByGCI* > ProvenanceRowsGCITuple;

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
    addColumn("i_p1_id");
    addColumn("i_p2_id");
    addColumn("i_p3_id");
    addColumn("dataset_i_id");
    addColumn("project_i_id");
    addColumn("i_name");
    addColumn("dataset_name");
    addColumn("io_logical_time_batch");
    addColumn("io_timestamp_batch");

    addRecoveryIndex("logical_time");
    addWatchEvent(NdbDictionary::Event::TE_INSERT);
  }

  FileProvenanceRow getRow(NdbRecAttr* value[]) {
    LOG_DEBUG("Get file provenance row: ");
    FileProvenanceRow row;
    row.mEventCreationTime = Utils::getCurrentTime();
    row.mInodeId = value[0]->int64_value();
    row.mOperation = get_string(value[1]);
    row.mLogicalTime = value[2]->int32_value();
    row.mTimestamp = value[3]->int64_value();
    row.mAppId = get_string(value[4]);
    row.mUserId = value[5]->int32_value();
    row.mPartitionId = value[6]->int64_value();
    row.mP1Id = value[7]->int64_value();
    row.mP2Id = value[8]->int64_value();
    row.mP3Id = value[9]->int64_value();
    row.mDatasetId = value[10]->int64_value();
    row.mProjectId = value[11]->int64_value();
    row.mInodeName = get_string(value[12]);
    row.mDatasetName = get_string(value[13]);
    row.mLogicalTimeBatch = value[14]->int32_value();
    row.mTimestampBatch = value[15]->int64_value();
    
    LOG_DEBUG("Got file provenance row: ");
    return row;
  }

  void removeLogs(Ndb* connection, PKeys& pks) {
    start(connection);
    for (PKeys::iterator it = pks.begin(); it != pks.end(); ++it) {
      FileProvenancePK pk = *it;
      AnyMap a;
      a[0] = pk.mInodeId;
      a[1] = pk.mOperation;
      a[2] = pk.mLogicalTime;
      a[3] = pk.mTimestamp;
      a[4] = pk.mAppId;
      a[5] = pk.mUserId;
      
      doDelete(a);
      LOG_DEBUG("Delete file provenance row: " << pk.to_string());
    }
    end();
  }

};


#endif /* FILEPROVENANCELOGTABLE_H */

