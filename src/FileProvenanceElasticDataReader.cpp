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

FileProvenanceElasticDataReader::FileProvenanceElasticDataReader(SConn connection, const bool hopsworks, const int lru_cap)
: NdbDataReader(connection, hopsworks), mInodeTable(lru_cap) {
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

string FileProvenanceElasticDataReader::process_row(FileProvenanceRow row) {
  LOG_INFO("reading features inode:" << row.mInodeId);
  
  if(row.mDatasetName == FileProvenanceConstants::ML_MODEL_DATASET) {
    boost::tuple<string, string, string> model = processModelComp(row);
    return bulk_add_json(row, boost::get<0>(model), boost::get<1>(model), boost::get<2>(model));
  } else {
    return bulk_add_json(row, FileProvenanceConstants::ML_TYPE_NONE, "", "");
  }
}

boost::tuple<string, string, string> FileProvenanceElasticDataReader::processModelComp(
  FileProvenanceRow row) {
  //the model(versioned) is the second directory within the "Models" dataset
  if(!row.mDatasetId == row.mP2Id) {
    return boost::make_tuple(FileProvenanceConstants::ML_TYPE_NONE, "", "");
  }
  boost::optional<XAttrRow> mlIdXAttr = getXAttr(XAttrPK(row.mInodeId, 
    FileProvenanceConstants::XATTRS_USER_NAMESPACE, FileProvenanceConstants::XATTRS_ML_ID));
  if(!mlIdXAttr) {
    return boost::make_tuple(FileProvenanceConstants::ML_TYPE_ERR, "", "");
  }
  string ml_deps = "";
  boost::optional<XAttrRow> mlDepsXAttr = getXAttr(XAttrPK(row.mInodeId, 
    FileProvenanceConstants::XATTRS_USER_NAMESPACE, FileProvenanceConstants::XATTRS_TRAINING_DATASETS));
  if(!mlDepsXAttr) {
    //return boost::make_tuple(FileProvenanceConstants::ML_TYPE_ERR, "", "");
  } else {
    ml_deps = mlDepsXAttr.get().mValue;
  }
  string ml_type = FileProvenanceConstants::ML_TYPE_MODEL;
  string ml_id = mlModelId(mlIdXAttr.get());
  return boost::make_tuple(ml_type, ml_id, ml_deps);
}

string FileProvenanceElasticDataReader::mlModelId(XAttrRow mlIdXAttr) {
  rapidjson::Document d;
  d.Parse(mlIdXAttr.mValue.c_str());
  rapidjson::Value& spaceId = d[FileProvenanceConstants::ML_ID_SPACE.c_str()];
  rapidjson::Value& base = d[FileProvenanceConstants::ML_ID_BASE.c_str()];
  rapidjson::Value& version = d[FileProvenanceConstants::ML_ID_VERSION.c_str()];
  stringstream ml_id;
  ml_id << spaceId.GetString() << "_" << base.GetString() << "_" << version.GetString();
  return ml_id.str();
}

string FileProvenanceElasticDataReader::bulk_add_json(FileProvenanceRow row, 
  string mlType, string mlId, string mlDeps) {
  
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

  dataVal.AddMember("inode_id",         rapidjson::Value().SetInt64(row.mInodeId), dataAlloc);
  dataVal.AddMember("inode_operation",  rapidjson::Value().SetString(row.mOperation.c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("io_logical_time",  rapidjson::Value().SetInt(row.mLogicalTime), dataAlloc);
  dataVal.AddMember("io_timestamp",     rapidjson::Value().SetInt64(row.mTimestamp), dataAlloc);
  dataVal.AddMember("io_app_id",        rapidjson::Value().SetString(row.mAppId.c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("user_id",          rapidjson::Value().SetInt(row.mUserId), dataAlloc);
  dataVal.AddMember("project_i_id",     rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);
  dataVal.AddMember("dataset_i_id",     rapidjson::Value().SetInt64(row.mDatasetId), dataAlloc);
  dataVal.AddMember("i_name",           rapidjson::Value().SetString(row.mInodeName.c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("i_readable_t",     rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("ml_type",          rapidjson::Value().SetString(mlType.c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("ml_id",            rapidjson::Value().SetString(mlId.c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("ml_deps",          rapidjson::Value().SetString(mlDeps.c_str(), dataAlloc), dataAlloc);
    
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