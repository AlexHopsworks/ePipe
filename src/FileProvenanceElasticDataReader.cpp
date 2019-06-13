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
: NdbDataReader(connection, hopsworks) {
}

class ElasticHelper {
public:

  static string mlAlive(string id, FileProvenanceRow row, string mlId, string mlType) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), opAlloc), opAlloc);

    op.AddMember("update", opVal, opAlloc);

    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);

    dataVal.AddMember("inode_id",         rapidjson::Value().SetInt64(row.mInodeId), dataAlloc);
    dataVal.AddMember("i_readable_t",     rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_id",            rapidjson::Value().SetString(mlId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_type",          rapidjson::Value().SetString(mlType.c_str(), dataAlloc), dataAlloc);

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

  static string deleteState(string id) {

    rapidjson::Document req;
    req.SetObject();

    rapidjson::Value idVal(rapidjson::kObjectType);
    
    idVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), req.GetAllocator()), req.GetAllocator());

    req.AddMember("delete", idVal, req.GetAllocator());

    rapidjson::StringBuffer reqBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> reqWriter(reqBuffer);
    req.Accept(reqWriter);
    
    stringstream out;
    out << reqBuffer.GetString();
    return out.str();
  }

  static string createOp(string id, FileProvenanceRow row, string mlId, string mlType) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), opAlloc), opAlloc);

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
    dataVal.AddMember("io_user_id",       rapidjson::Value().SetInt(row.mUserId), dataAlloc);
    dataVal.AddMember("project_i_id",     rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);
    dataVal.AddMember("dataset_i_id",     rapidjson::Value().SetInt64(row.mDatasetId), dataAlloc);
    dataVal.AddMember("i_name",           rapidjson::Value().SetString(row.mInodeName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("i_readable_t",     rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_id",            rapidjson::Value().SetString(mlId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_type",          rapidjson::Value().SetString(mlType.c_str(), dataAlloc), dataAlloc);
     
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

  static string otherOp(string id, FileProvenanceRow row) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), opAlloc), opAlloc);

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
    dataVal.AddMember("io_user_id",       rapidjson::Value().SetInt(row.mUserId), dataAlloc);
    dataVal.AddMember("i_readable_t",     rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
        
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

  static string xattrOp(string id, FileProvenanceRow row, string val) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), opAlloc), opAlloc);

    op.AddMember("update", opVal, opAlloc);

    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);

    dataVal.AddMember("inode_id",         rapidjson::Value().SetInt64(row.mInodeId), dataAlloc);
    dataVal.AddMember("inode_operation",  rapidjson::Value().SetString(row.mOperation.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("io_logical_time",  rapidjson::Value().SetInt(row.mLogicalTime), dataAlloc);
    dataVal.AddMember("io_timestamp",     rapidjson::Value().SetInt64(row.mTimestamp), dataAlloc);
    dataVal.AddMember("i_readable_t",     rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
    rapidjson::Value rname(row.mXAttrName.c_str(), dataAlloc);
    rapidjson::Value rval(val.c_str(), dataAlloc);
    dataVal.AddMember(rname, rval, dataAlloc);
      
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

  static string readable_timestamp(Int64 timestamp) {
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

  static string opId(FileProvenanceRow row) {
    stringstream out;
    out << row.mInodeId << "-" << row.mOperation << "-" << row.mLogicalTime << "-" << row.mTimestamp << "-" << row.mAppId << "-" << row.mUserId;
    return out.str();
  }

  static string stateId(FileProvenanceRow row) {
    stringstream out;
    out << row.mInodeId;
    return out.str();
  }
};

class XAttrBufferReader {
public:
  XAttrBufferReader(SConn conn) : mConn(conn) {
  }

  boost::optional<FPXAttrBufferRow> getXAttr(FPXAttrBufferPK key) {
    boost::optional<FPXAttrBufferRow> row = mXAttr.get(mConn, key);
    if(row) {
      LOG_INFO("retrieved xattr:" << key.mName);
    } 
    return row;
  }
  
private:
  FileProvenanceXAttrBufferTable mXAttr;
  SConn mConn;
};

void FileProvenanceElasticDataReader::processAddedandDeleted(Pq* data_batch, Bulk<ProvKeys>& bulk) {
  vector<ptime> arrivalTimes(data_batch->size());
  stringstream out;
  int i = 0;
  for (Pq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
    FileProvenanceRow row = *it;
    arrivalTimes[i] = row.mEventCreationTime;
    std::list<boost::tuple<string, boost::optional<FileProvenancePK>, boost::optional<FPXAttrBufferPK> > > result = process_row(row);
    for(boost::tuple<string, boost::optional<FileProvenancePK>, boost::optional<FPXAttrBufferPK> > item : result) {
      boost::optional<FileProvenancePK> fpPK = boost::get<1>(item);
      boost::optional<FPXAttrBufferPK> fpXAttrBufferPK = boost::get<2>(item);
      bulk.mPKs.mFileProvLogKs.push_back(fpPK);
      bulk.mPKs.mXAttrBufferKs.push_back(fpXAttrBufferPK);
      out << boost::get<0>(item) << endl;
    }
  }
  bulk.mArrivalTimes = arrivalTimes;
  bulk.mJSON = out.str();
}

std::list<boost::tuple<string, boost::optional<FileProvenancePK>, boost::optional<FPXAttrBufferPK> > > FileProvenanceElasticDataReader::process_row(FileProvenanceRow row) {
  LOG_INFO("reading provenance for inode:" << row.mInodeId);
  std::list<boost::tuple<string, boost::optional<FileProvenancePK>, boost::optional<FPXAttrBufferPK> > > result;
  if(row.mOperation == FileProvenanceConstants::H_OP_XATTR_ADD) {
    FPXAttrBufferPK xattrBufferKey(row.mInodeId, 0, row.mXAttrName, row.mLogicalTime);
    boost::optional<FPXAttrBufferRow> xAttrBufferVal = mXAttr.get(mNdbConnection, xattrBufferKey);
    string val;
    if(xAttrBufferVal) {
      val = ElasticHelper::xattrOp(ElasticHelper::opId(row), row, xAttrBufferVal.get().mValue);
    } else {
      stringstream cause;
      cause << "no such xattr in buffer: " << row.mXAttrName;
      throw cause.str();
    }
    result.push_back(boost::make_tuple(val, row.getPK(), xattrBufferKey));
  } else {
    if(row.mOperation == FileProvenanceConstants::H_OP_CREATE) {
      string mlType = FileProvenanceConstants::ML_TYPE_NONE;
      string mlId = "";
      LOG_INFO("mlType: " << row.to_string());
      if(FileProvenanceConstants::isMLModel(row)) {
        LOG_INFO("mlType: model");
        mlType = FileProvenanceConstants::ML_TYPE_MODEL;
        mlId = FileProvenanceConstants::getMLModelId(row);
      } else if(FileProvenanceConstants::isMLTDataset(row)) {
        LOG_INFO("mlType: dataset");
        mlType = FileProvenanceConstants::ML_TYPE_TDATASET;
        mlId = FileProvenanceConstants::getMLTDatasetId(row);
      } else if(FileProvenanceConstants::isMLFeature(row)) {
        LOG_INFO("mlType: feature");
        mlType = FileProvenanceConstants::ML_TYPE_FEATURE;
        mlId = FileProvenanceConstants::getMLFeatureId(row);
      } else if(FileProvenanceConstants::isMLExperiment(row)) {
        LOG_INFO("mlType: experiment");
        mlType = FileProvenanceConstants::ML_TYPE_EXPERIMENT;
        mlId = FileProvenanceConstants::getMLExperimentId(row);
        string state = ElasticHelper::createMLState(ElasticHelper::stateId(row), row, mlId, mlType);
        result.push_back(boost::make_tuple(state, boost::none, boost::none));
      } else if(FileProvenanceConstants::partOfMLModel(row)) {
        LOG_INFO("mlType: model part");
        mlType = FileProvenanceConstants::ML_TYPE_MODEL_PART;
        mlId = FileProvenanceConstants::getMLModelParentId(row);
      } else if(FileProvenanceConstants::partOfMLTDataset(row)) {
        LOG_INFO("mlType: dataset part");
        mlType = FileProvenanceConstants::ML_TYPE_TDATASET_PART;
        mlId = FileProvenanceConstants::getMLTDatasetParentId(row);
      } else if(FileProvenanceConstants::partOfMLFeature(row)) {
        LOG_INFO("mlType: feature part");
        mlType = FileProvenanceConstants::ML_TYPE_FEATURE_PART;
        mlId = FileProvenanceConstants::getMLFeatureParentId(row);
      } else if(FileProvenanceConstants::partOfMLExperiment(row)) {
        LOG_INFO("mlType: experiment part");
        mlType = FileProvenanceConstants::ML_TYPE_EXPERIMENT_PART;
        mlId = FileProvenanceConstants::getMLExperimentParentId(row);
      } else {
        LOG_INFO("mlType: none");
      }
      string op = ElasticHelper::createOp(ElasticHelper::opId(row), row, mlId, mlType);
      result.push_back(boost::make_tuple(op, row.getPK(), boost::none));
    } else if(row.mOperation == FileProvenanceConstants::H_OP_DELETE) {
      string op = ElasticHelper::otherOp(ElasticHelper::opId(row), row);
      result.push_back(boost::make_tuple(op, row.getPK(), boost::none));
      string state = ElasticHelper::deleteState(ElasticHelper::stateId(row));
      result.push_back(boost::make_tuple(state, boost::none, boost::none));
    } else {
      string op = ElasticHelper::otherOp(ElasticHelper::opId(row), row);
      result.push_back(boost::make_tuple(op, row.getPK(), boost::none));
    }
  }
  return result;
}

FileProvenanceElasticDataReader::~FileProvenanceElasticDataReader() {
  
}