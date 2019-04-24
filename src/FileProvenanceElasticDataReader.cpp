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
 * File:   FileProvenanceElasticDataReader.cpp
 * Author: Alexandru Ormenisan <aaor@kth.se>
 * 
 */

#include "FileProvenanceElasticDataReader.h"

FileProvenanceElasticDataReader::FileProvenanceElasticDataReader(SConn connection, const bool hopsworks)
: NdbDataReader(connection, hopsworks) {
}

void FileProvenanceElasticDataReader::processAddedandDeleted(Pq* data_batch, Bulk<PKeys>& bulk) {
  vector<ptime> arrivalTimes(data_batch->size());
  stringstream out;
  int i = 0;
  for (Pq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
    FileProvenanceRow row = *it;
    arrivalTimes[i] = row.mEventCreationTime;
    FileProvenancePK rowPK = row.getPK();
    bulk.mPKs.push_back(rowPK);
    out << process_row(row) << endl;
  }

  bulk.mArrivalTimes = arrivalTimes;
  bulk.mJSON = out.str();
}

boost::optional<XAttrRow> FileProvenanceElasticDataReader::getXAttr(XAttrPK key) {
  boost::optional<XAttrRow> row = mXAttrTable.get(mNdbConnection, key);
  if(row) {
    LOG_INFO("retrieved " << key.mName << " from hdfs_xattrs");
  } else {
    row = mXAttrTrashBinTable.get(mNdbConnection, key);
    if(row) {
      LOG_INFO("retrieved " << key.mName << " from hdfs_xattrs trashbin");
    } else {
      return  boost::none;
    }
  }
  return row;
}

boost::optional<XAttrRow> FileProvenanceElasticDataReader::getFeatures(Int64 inodeId) {
  XAttrPK key = XAttrPK(inodeId, XATTRS_USER_NAMESPACE, XATTRS_FEATURES);
  return getXAttr(key);
}

boost::optional<XAttrRow> FileProvenanceElasticDataReader::getTrainingDatasets(Int64 inodeId) {
  XAttrPK key = XAttrPK(inodeId, XATTRS_USER_NAMESPACE, XATTRS_TRAINING_DATASETS);
  return getXAttr(key);
}

string FileProvenanceElasticDataReader::process_row(FileProvenanceRow row) {
  LOG_INFO("reading features inode:" << row.mInodeId);
  boost::optional<XAttrRow> features = getFeatures(row.mInodeId);
  if(features) {
    LOG_INFO("has features");
  } else {
    LOG_INFO("no features");
  }
  
  return bulk_add_json(row);
}

string FileProvenanceElasticDataReader::bulk_add_json(FileProvenanceRow row) {
  rapidjson::Document op;
  op.SetObject();
  rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

  rapidjson::Value opVal(rapidjson::kObjectType);
  opVal.AddMember("_id", rapidjson::Value().SetString(row.getPK().to_string().c_str(), opAlloc), opAlloc);

  op.AddMember("update", opVal, opAlloc);

  rapidjson::Document data;
  data.SetObject();
  rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

  rapidjson::Value dataVal(rapidjson::kObjectType);

  dataVal.AddMember("inode_id",             rapidjson::Value().SetInt64(row.mInodeId), dataAlloc);
  dataVal.AddMember("inode_operation",      rapidjson::Value().SetString(row.mOperation.c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("io_logical_time",      rapidjson::Value().SetInt(row.mLogicalTime), dataAlloc);
  dataVal.AddMember("io_timestamp",         rapidjson::Value().SetInt64(row.mTimestamp), dataAlloc);
  dataVal.AddMember("io_app_id",            rapidjson::Value().SetString(row.mAppId.c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("user_id",              rapidjson::Value().SetInt(row.mUserId), dataAlloc);
  dataVal.AddMember("project_i_id",         rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);
  dataVal.AddMember("dataset_i_id",         rapidjson::Value().SetInt64(row.mDatasetId), dataAlloc);
  dataVal.AddMember("i_name",               rapidjson::Value().SetString(row.mInodeName.c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("i_readable_t",         rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("ml_type",              rapidjson::Value().SetString("none", dataAlloc), dataAlloc);
  dataVal.AddMember("ml_id",                rapidjson::Value().SetInt64(0l), dataAlloc);
  dataVal.AddMember("ml_parent",            rapidjson::Value().SetInt64(0l), dataAlloc);
  dataVal.AddMember("ml_deps",              rapidjson::Value().SetInt64(0l), dataAlloc);
    
  data.AddMember("doc", dataVal, dataAlloc);
  data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);

  rapidjson::StringBuffer opBuffer;
  rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
  op.Accept(opWriter);

  rapidjson::StringBuffer dataBuffer;
  rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
  data.Accept(dataWriter);
  
  stringstream out;
  out << opBuffer.GetString() << endl << dataBuffer.GetString();
  return out.str();
}

string FileProvenanceElasticDataReader::readable_timestamp(Int64 timestamp) {
  using namespace boost::posix_time;
  using namespace boost::gregorian;
  time_t raw_t = (time_t)timestamp/1000; //time_t is time in seconds?
  ptime p_timestamp = from_time_t(raw_t);
  stringstream t_date;
  t_date << p_timestamp.date().year() << "." << p_timestamp.date().month() << "." << p_timestamp.date().day();
  stringstream t_time;
  t_time << p_timestamp.time_of_day().hours() << ":" << p_timestamp.time_of_day().minutes() << ":" << p_timestamp.time_of_day().seconds();
  stringstream readable_timestamp;
  readable_timestamp << t_date.str().c_str() << " " << t_time.str().c_str();
  return readable_timestamp.str();
}

FileProvenanceElasticDataReader::~FileProvenanceElasticDataReader() {
  
}