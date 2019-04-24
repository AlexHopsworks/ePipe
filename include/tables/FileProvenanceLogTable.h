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

struct FileProvenancePK {
  Int64 mInodeId;
  string mOperation;
  int mLogicalTime;
  string mAppId;
  int mUserId;
  Int64 mTimestamp;
  
  FileProvenancePK(Int64 inodeId, string operation, int logicalTime, string appId, int userId, Int64 timestamp) {
    mInodeId = inodeId;
    mOperation = operation;
    mLogicalTime = logicalTime;
    mAppId = appId;
    mUserId = userId;
    mTimestamp = timestamp;
  }

  string to_string() {
    stringstream out;
    out << mInodeId << "-" << mOperation << "-" << mLogicalTime << "-" << mAppId << "-" << mUserId << "-" << mTimestamp;
    return out.str();
  }
};

struct FileProvenanceRow {
  Int64 mInodeId;
  string mOperation;
  int mLogicalTime;
  string mAppId;
  int mUserId;
  Int64 mTimestamp;
  Int64 mParentId;
  Int64 mProjectId;
  Int64 mDatasetId;
  string mInodeName;
  int mLogicalTimeBatch;
  Int64 mTimestampBatch;
  Int64 mPartitionId;
  string mPath;
  ptime mEventCreationTime;

  FileProvenancePK getPK() {
    return FileProvenancePK(mInodeId, mOperation, mLogicalTime, mAppId, mUserId, mTimestamp);
  }

  string to_string() {
    stringstream stream;
    stream << "-------------------------" << endl;
    stream << "InodeId = " << mInodeId << endl;
    stream << "Operation = " << mOperation << endl;
    stream << "LogicalTime = " << mLogicalTime << endl;
    stream << "AppId = " << mAppId << endl;
    stream << "UserId = " << mUserId << endl;
    stream << "Timestamp = " << mTimestamp << endl;
    stream << "ParentId = " << mParentId << endl;
    stream << "ProjectId = " << mProjectId << endl;
    stream << "DatasetId = " << mDatasetId << endl;
    stream << "InodeName = " << mInodeName << endl;
    stream << "LogicalTimeBatch = " << mLogicalTimeBatch << endl;
    stream << "TimestampBatch = " << mTimestampBatch << endl;
    stream << "PartitionId = " << mPartitionId << endl;
    stream << "Path = " << mPath << endl;
    stream << "-------------------------" << endl;
    return stream.str();
  }

  string to_create_json() {
    stringstream out;
    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("update");
    opWriter.StartObject();

    opWriter.String("_id");
    opWriter.String(getPK().to_string().c_str());

    opWriter.EndObject();

    opWriter.EndObject();

    out << sbOp.GetString() << endl;

    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);

    docWriter.StartObject();
    docWriter.String("doc");
    docWriter.StartObject();

    docWriter.String("inode_id");
    docWriter.Int64(mInodeId);

    docWriter.String("user_id");
    docWriter.Int(mUserId);

    docWriter.String("app_id");
    docWriter.String(mAppId.c_str());

    docWriter.String("logical_time");
    docWriter.Int(mLogicalTime);

    docWriter.String("partition_id");
    docWriter.Int64(mPartitionId);

    docWriter.String("parent_id");
    docWriter.Int64(mParentId);

    docWriter.String("project_id");
    docWriter.Int64(mProjectId);

    docWriter.String("dataset_id");
    docWriter.Int64(mDatasetId);

    docWriter.String("inode_name");
    docWriter.String(mInodeName.c_str());

    docWriter.String("logical_time_batch");
    docWriter.Int(mLogicalTimeBatch);

    docWriter.String("timestamp");
    docWriter.Int64(mTimestamp);

    docWriter.String("timestamp_batch");
    docWriter.Int64(mTimestampBatch);

    docWriter.String("operation");
    docWriter.String(mOperation.c_str());

    docWriter.EndObject();

    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);

    docWriter.EndObject();

    out << sbDoc.GetString() << endl;
    return out.str();
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
    boost::hash_combine(seed, a.mLogicalTime);
    boost::hash_combine(seed, a.mOperation);
    boost::hash_combine(seed, a.mAppId);
    boost::hash_combine(seed, a.mUserId);
    boost::hash_combine(seed, a.mTimestamp);
    
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
    addColumn("operation");
    addColumn("logical_time");
    addColumn("app_id");
    addColumn("user_id");
    addColumn("timestamp");
    addColumn("parent_id");
    addColumn("project_id");
    addColumn("dataset_id");
    addColumn("inode_name");
    addColumn("logical_time_batch");
    addColumn("timestamp_batch");
    addColumn("partition_id");
    addColumn("path");
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
    row.mAppId = get_string(value[3]);
    row.mUserId = value[4]->int32_value();
    row.mTimestamp = value[5]->int64_value();
    row.mParentId = value[6]->int64_value();
    row.mProjectId = value[7]->int64_value();
    row.mDatasetId = value[8]->int64_value();
    row.mInodeName = get_string(value[9]);
    row.mLogicalTimeBatch = value[10]->int32_value();
    row.mTimestampBatch = value[11]->int64_value();
    row.mPartitionId = value[12]->int64_value();
    row.mPath = get_string(value[13]);
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
      a[3] = pk.mAppId;
      a[4] = pk.mUserId;
      a[5] = pk.mTimestamp;
      
      doDelete(a);
      LOG_DEBUG("Delete file provenance row: " << pk.to_string());
    }
    end();
  }

};


#endif /* FILEPROVENANCELOGTABLE_H */

