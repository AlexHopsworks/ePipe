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

#include "FileProvenanceElasticDataReader.h"

FileProvenanceElasticDataReader::FileProvenanceElasticDataReader(SConn connection, const bool hopsworks)
: NdbDataReader(connection, hopsworks) {
}

class ElasticHelper {
public:

  static std::string aliveState(std::string id, FileProvenanceRow row, std::string mlId, FileProvenanceConstants::MLType mlType) {
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
    dataVal.AddMember("create_timestamp", rapidjson::Value().SetInt64(row.mTimestamp), dataAlloc);
    dataVal.AddMember("app_id",           rapidjson::Value().SetString(row.mAppId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("user_id",          rapidjson::Value().SetInt(row.mUserId), dataAlloc);
    dataVal.AddMember("project_i_id",     rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);
    dataVal.AddMember("dataset_i_id",     rapidjson::Value().SetInt64(row.mDatasetId), dataAlloc);
    dataVal.AddMember("parent_i_id",      rapidjson::Value().SetInt64(row.mParentId), dataAlloc);
    dataVal.AddMember("inode_name",       rapidjson::Value().SetString(row.mInodeName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("project_name",     rapidjson::Value().SetString(row.mProjectName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_id",            rapidjson::Value().SetString(mlId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_type",          rapidjson::Value().SetString(FileProvenanceConstants::MLTypeToStr(mlType).c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("entry_type",       rapidjson::Value().SetString("state", dataAlloc), dataAlloc);
    dataVal.AddMember("partition_id",     rapidjson::Value().SetInt64(row.mPartitionId), dataAlloc);
    dataVal.AddMember("r_create_timestamp",    rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
    
    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);
    
    std::stringstream out;
    out << opBuffer.GetString() << std::endl << dataBuffer.GetString();
    return out.str();
  }

  static std::string addXAttrToState(std::string id, FileProvenanceRow row, std::string val) {
    //id
    rapidjson::Document cleanupId;
    cleanupId.SetObject();

    rapidjson::Value cleanupIdVal(rapidjson::kObjectType);
    cleanupIdVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), cleanupId.GetAllocator()), cleanupId.GetAllocator());

    cleanupId.AddMember("update", cleanupIdVal, cleanupId.GetAllocator());

    //clean previous
    rapidjson::Document cleanup;
    cleanup.SetObject();

    std::stringstream script;
    script << "if(ctx._source.containsKey(\"" << row.mXAttrName << "\")){ ";
    script << "ctx._source.remove(\"" << row.mXAttrName <<  "\");";
    script << "} else{ ctx.op=\"noop\";}";

    rapidjson::Value scriptVal(script.str().c_str(), cleanup.GetAllocator());
    cleanup.AddMember("script", scriptVal, cleanup.GetAllocator());

    //id
    rapidjson::Document op;
    op.SetObject();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), op.GetAllocator()), op.GetAllocator());

    op.AddMember("update", opVal, op.GetAllocator());

    //update
    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);

    rapidjson::Value xattrKey(row.mXAttrName.c_str(), dataAlloc);
    rapidjson::Value xattr(rapidjson::kObjectType);
    rapidjson::Value xAttrAux1(val.c_str(), dataAlloc);
    xattr.AddMember("raw", xAttrAux1, dataAlloc);

    rapidjson::Document xattrJson(&data.GetAllocator()); 
    if(!xattrJson.Parse(val.c_str()).HasParseError()) {     
      xattr.AddMember("value", xattrJson.Move(), dataAlloc);
    }
    dataVal.AddMember(xattrKey, xattr, dataAlloc);

    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);
    //done
    rapidjson::StringBuffer cleanupIdBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> cleanupIdWriter(cleanupIdBuffer);
    cleanupId.Accept(cleanupIdWriter);

    rapidjson::StringBuffer cleanupBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> cleanupWriter(cleanupBuffer);
    cleanup.Accept(cleanupWriter);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);
    
    std::stringstream out;
    out << cleanupIdBuffer.GetString()  << std::endl 
        << cleanupBuffer.GetString()    << std::endl 
        << opBuffer.GetString()         << std::endl 
        << dataBuffer.GetString();
    LOG_INFO("result " << out.str());
    return out.str();
  }

  static std::string deleteXAttrFromState(std::string id, FileProvenanceRow row) {
    //id
    rapidjson::Document cleanupId;
    cleanupId.SetObject();

    rapidjson::Value cleanupIdVal(rapidjson::kObjectType);
    cleanupIdVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), cleanupId.GetAllocator()), cleanupId.GetAllocator());
    cleanupId.AddMember("update", cleanupIdVal, cleanupId.GetAllocator());

    //cleanup
    rapidjson::Document cleanup;
    cleanup.SetObject();

    std::stringstream script;
    script << "if(ctx._source.containsKey(\"" << row.mXAttrName << "\")){ ";
    script << "ctx._source.remove(\"" << row.mXAttrName <<  "\");";
    script << "} else{ ctx.op=\"noop\";}";

    rapidjson::Value scriptVal(script.str().c_str(), cleanup.GetAllocator());
    cleanup.AddMember("script", scriptVal, cleanup.GetAllocator());

    //done
    rapidjson::StringBuffer cleanupIdBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> cleanupIdWriter(cleanupIdBuffer);
    cleanupId.Accept(cleanupIdWriter);

    rapidjson::StringBuffer cleanupBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> cleanupWriter(cleanupBuffer);
    cleanup.Accept(cleanupWriter);
    
    std::stringstream out;
    out << cleanupIdBuffer.GetString() << std::endl 
        << cleanupBuffer.GetString();
    return out.str();
  }

  static std::string deadState(std::string id) {

    rapidjson::Document req;
    req.SetObject();

    rapidjson::Value idVal(rapidjson::kObjectType);
    
    idVal.AddMember("_id", rapidjson::Value().SetString(id.c_str(), req.GetAllocator()), req.GetAllocator());

    req.AddMember("delete", idVal, req.GetAllocator());

    rapidjson::StringBuffer reqBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> reqWriter(reqBuffer);
    req.Accept(reqWriter);
    
    std::stringstream out;
    out << reqBuffer.GetString();
    return out.str();
  }

  static std::string fileOp(std::string id, FileProvenanceRow row, std::string mlId, FileProvenanceConstants::MLType mlType) {
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
    dataVal.AddMember("logical_time",     rapidjson::Value().SetInt(row.mLogicalTime), dataAlloc);
    dataVal.AddMember("timestamp",        rapidjson::Value().SetInt64(row.mTimestamp), dataAlloc);
    dataVal.AddMember("app_id",           rapidjson::Value().SetString(row.mAppId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("user_id",           rapidjson::Value().SetInt(row.mUserId), dataAlloc);
    dataVal.AddMember("project_i_id",     rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);
    dataVal.AddMember("dataset_i_id",     rapidjson::Value().SetInt64(row.mDatasetId), dataAlloc);
    dataVal.AddMember("parent_i_id",      rapidjson::Value().SetInt64(row.mParentId), dataAlloc);
    dataVal.AddMember("inode_name",       rapidjson::Value().SetString(row.mInodeName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("project_name",     rapidjson::Value().SetString(row.mProjectName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_id",            rapidjson::Value().SetString(mlId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_type",          rapidjson::Value().SetString(FileProvenanceConstants::MLTypeToStr(mlType).c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("entry_type",       rapidjson::Value().SetString("operation", dataAlloc), dataAlloc);
    dataVal.AddMember("partition_id",     rapidjson::Value().SetInt64(row.mPartitionId), dataAlloc);
    dataVal.AddMember("r_timestamp",      rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
     
    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);
    
    std::stringstream out;
    out << opBuffer.GetString() << std::endl << dataBuffer.GetString();
    return out.str();
  }

  static std::string addXAttrOp(std::string id, FileProvenanceRow row, std::string val, std::string mlId, FileProvenanceConstants::MLType mlType) {
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
    dataVal.AddMember("logical_time",     rapidjson::Value().SetInt(row.mLogicalTime), dataAlloc);
    dataVal.AddMember("timestamp",        rapidjson::Value().SetInt64(row.mTimestamp), dataAlloc);
    dataVal.AddMember("app_id",           rapidjson::Value().SetString(row.mAppId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("user_id",           rapidjson::Value().SetInt(row.mUserId), dataAlloc);
    dataVal.AddMember("project_i_id",     rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);
    dataVal.AddMember("dataset_i_id",     rapidjson::Value().SetInt64(row.mDatasetId), dataAlloc);
    dataVal.AddMember("parent_i_id",      rapidjson::Value().SetInt64(row.mParentId), dataAlloc);
    dataVal.AddMember("inode_name",       rapidjson::Value().SetString(row.mInodeName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("project_name",     rapidjson::Value().SetString(row.mProjectName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_id",            rapidjson::Value().SetString(mlId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_type",          rapidjson::Value().SetString(FileProvenanceConstants::MLTypeToStr(mlType).c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("entry_type",       rapidjson::Value().SetString("operation", dataAlloc), dataAlloc);
    dataVal.AddMember("partition_id",     rapidjson::Value().SetInt64(row.mPartitionId), dataAlloc);
    dataVal.AddMember("r_timestamp",      rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
     
    rapidjson::Value xattr(rapidjson::kObjectType);
    xattr.AddMember("raw", rapidjson::Value().SetString(val.c_str(), dataAlloc), dataAlloc);
    rapidjson::Document xattrJson(&data.GetAllocator());
    if(!xattrJson.Parse(val.c_str()).HasParseError()) {     
      xattr.AddMember("value", xattrJson.Move(), dataAlloc);
    }
    // rapidjson::Value xattrKey(row.mXAttrName.c_str(), dataAlloc);
    dataVal.AddMember(rapidjson::Value().SetString(row.mXAttrName.c_str(), dataAlloc), xattr, dataAlloc);
    // dataVal.AddMember(xattrKey, xattr, dataAlloc);
    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);
    
    std::stringstream out;
    out << opBuffer.GetString() << std::endl << dataBuffer.GetString();
    LOG_INFO("json5:" <<out.str());
    return out.str();
  }

  static std::string deleteXAttrOp(std::string id, FileProvenanceRow row, std::string mlId, FileProvenanceConstants::MLType mlType) {
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
    dataVal.AddMember("logical_time",     rapidjson::Value().SetInt(row.mLogicalTime), dataAlloc);
    dataVal.AddMember("timestamp",        rapidjson::Value().SetInt64(row.mTimestamp), dataAlloc);
    dataVal.AddMember("app_id",           rapidjson::Value().SetString(row.mAppId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("user_id",           rapidjson::Value().SetInt(row.mUserId), dataAlloc);
    dataVal.AddMember("project_i_id",     rapidjson::Value().SetInt64(row.mProjectId), dataAlloc);
    dataVal.AddMember("dataset_i_id",     rapidjson::Value().SetInt64(row.mDatasetId), dataAlloc);
    dataVal.AddMember("parent_i_id",      rapidjson::Value().SetInt64(row.mParentId), dataAlloc);
    dataVal.AddMember("inode_name",       rapidjson::Value().SetString(row.mInodeName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("project_name",     rapidjson::Value().SetString(row.mProjectName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_id",            rapidjson::Value().SetString(mlId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("ml_type",          rapidjson::Value().SetString(FileProvenanceConstants::MLTypeToStr(mlType).c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("entry_type",       rapidjson::Value().SetString("operation", dataAlloc), dataAlloc);
    dataVal.AddMember("partition_id",     rapidjson::Value().SetInt64(row.mPartitionId), dataAlloc);
    dataVal.AddMember("r_timestamp",      rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
  
    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);
    
    std::stringstream out;
    out << opBuffer.GetString() << std::endl << dataBuffer.GetString();
    return out.str();
  }

  static std::string readable_timestamp(Int64 timestamp) {
    using namespace boost::posix_time;
    using namespace boost::gregorian;
    time_t raw_t = (time_t)timestamp/1000; //time_t is time in seconds?
    ptime p_timestamp = from_time_t(raw_t);
    std::stringstream t_date;
    t_date << p_timestamp.date().year() << "." << p_timestamp.date().month() << "." << p_timestamp.date().day();
    std::stringstream t_time;
    t_time << p_timestamp.time_of_day().hours() << ":" << p_timestamp.time_of_day().minutes() << ":" << p_timestamp.time_of_day().seconds();
    std::stringstream readable_timestamp;
    readable_timestamp << t_date.str().c_str() << " " << t_time.str().c_str();
    return readable_timestamp.str();
  }

  static std::string opId(FileProvenanceRow row) {
    std::stringstream out;
    out << row.mInodeId << "-" << row.mOperation << "-" << row.mLogicalTime << "-" << row.mTimestamp << "-" << row.mAppId << "-" << row.mUserId;
    return out.str();
  }

  static std::string stateId(FileProvenanceRow row) {
    std::stringstream out;
    out << row.mInodeId;
    return out.str();
  }
};

void FileProvenanceElasticDataReader::processAddedandDeleted(Pq* data_batch, eBulk& bulk) {
  std::stringstream out;
  for (Pq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
    FileProvenanceRow row = *it;
    ProcessRowResult result = process_row(row);
    std::list<std::string> bulkOps = boost::get<1>(result);
    if(bulkOps.empty()) {
      out << FileProvenanceConstants::ELASTIC_NOP;
    } else {
      for(std::string op : bulkOps) {
        out << op << std::endl;
      }
    }
    bulk.push(mFileLogTable.getLogHandler(boost::get<2>(result), boost::get<3>(result)), row.mEventCreationTime, out.str());
  }
}

ProcessRowResult FileProvenanceElasticDataReader::process_row(FileProvenanceRow row) {
  LOG_DEBUG("reading provenance for inode:" << row.mInodeId);
  std::list<std::string> bulkOps;
  FileProvenanceConstants::Operation op = FileProvenanceConstants::findOp(row);
  std::pair<FileProvenanceConstants::MLType, std::string> mlAux = FileProvenanceConstants::parseML(row);
  boost::optional<FileProvenanceConstants::ProvOpStoreType> datasetProvCore = datasetProvCore(row);
  switch (op) {
    case FileProvenanceConstants::Operation::OP_CREATE: {
      switch (mlAux.first) {
        case FileProvenanceConstants::MLType::MODEL:
        case FileProvenanceConstants::MLType::FEATURE:
        case FileProvenanceConstants::MLType::TRAINING_DATASET:
        case FileProvenanceConstants::MLType::EXPERIMENT:
        case FileProvenanceConstants::MLType::DATASET: {
          std::string state = ElasticHelper::aliveState(ElasticHelper::stateId(row), row, mlAux.second, mlAux.first);
          bulkOps.push_back(state);
        } break;
        default: ;break; //do nothing
      }
      if (datasetProvCore && datasetProvCore.get() == FileProvenanceConstants::ProvOpStoreType::STORE_ALL) {
        std::string op = ElasticHelper::fileOp(ElasticHelper::opId(row), row, mlAux.second, mlAux.first);
        bulkOps.push_back(op);
      }
    } break;
    case FileProvenanceConstants::Operation::OP_DELETE: {
      switch (mlAux.first) {
        case FileProvenanceConstants::MLType::MODEL:
        case FileProvenanceConstants::MLType::FEATURE:
        case FileProvenanceConstants::MLType::TRAINING_DATASET:
        case FileProvenanceConstants::MLType::EXPERIMENT:
        case FileProvenanceConstants::MLType::DATASET: {
          std::string state = ElasticHelper::deadState(ElasticHelper::stateId(row));
          bulkOps.push_back(state);
        } break;
        default: ;break; //do nothing
      }
      if (datasetProvCore && datasetProvCore.get() == FileProvenanceConstants::ProvOpStoreType::STORE_ALL) {
        std::string op = ElasticHelper::fileOp(ElasticHelper::opId(row), row, mlAux.second, mlAux.first);
        bulkOps.push_back(op);
      }
    } break;
    case FileProvenanceConstants::Operation::OP_MODIFY_DATA:
    case FileProvenanceConstants::Operation::OP_ACCESS_DATA: {
      if (datasetProvCore && datasetProvCore.get() == FileProvenanceConstants::ProvOpStoreType::STORE_ALL) {
        std::string op = ElasticHelper::fileOp(ElasticHelper::opId(row), row, mlAux.second, mlAux.first);
        bulkOps.push_back(op);
      }
    } break;
    case FileProvenanceConstants::Operation::OP_XATTR_ADD:
    case FileProvenanceConstants::Operation::OP_XATTR_UPDATE: {
      FPXAttrBufferPK xattrBufferKey(row.mInodeId, FileProvenanceConstants::XATTRS_USER_NAMESPACE, row.mXAttrName, row.mLogicalTime);
      FPXAttrBufferRow xattr = readBufferedXAttr(xattrBufferKey);
      if(row.mXAttrName == FileProvenanceConstants::PROV_CORE) {
        std::pair<FileProvenanceConstants::ProvOpStoreType, Int64> opProvCore = FileProvenanceConstants::provCore(xattr.mValue);
        switch (opProvCore.first) {
          case FileProvenanceConstants::ProvOpStoreType::STORE_NONE: {
            std::string state = ElasticHelper::deadState(ElasticHelper::stateId(row));
            bulkOps.push_back(state);
          } break;
          case FileProvenanceConstants::ProvOpStoreType::STORE_STATE:
          case FileProvenanceConstants::ProvOpStoreType::STORE_ALL: {
            std::string state = ElasticHelper::aliveState(ElasticHelper::stateId(row), row, mlAux.second, mlAux.first);
            bulkOps.push_back(state);
          } break;
          default: {
            LOG_ERROR("prov state - xattr add/update - add to state - ProvOpStoreType not handled" << FileProvenanceConstants::provOpStoreTypeToStr(provType));
            std::stringstream cause;
            cause << "prov state - xattr add/update - add to state - ProvOpStoreType not handled" << FileProvenanceConstants::provOpStoreTypeToStr(provType);
            throw std::logic_error(cause.str());
          }
        }
      }
      switch (mlAux.first) {
        case FileProvenanceConstants::MLType::DATASET:
        case FileProvenanceConstants::MLType::HIVE:
        case FileProvenanceConstants::MLType::FEATURE:
        case FileProvenanceConstants::MLType::TRAINING_DATASET:
        case FileProvenanceConstants::MLType::EXPERIMENT:
        case FileProvenanceConstants::MLType::MODEL:{
         switch(opProvCore.get().first) {
           case FileProvenanceConstants::ProvOpStoreType::STORE_ALL:
           case FileProvenanceConstants::ProvOpStoreType::STORE_STATE: {
             std::string xattrStateVal = ElasticHelper::addXAttrToState(ElasticHelper::stateId(row), row, xattr.mValue);
             bulkOps.push_back(xattrStateVal);
           } break;
           case FileProvenanceConstants::ProvOpStoreType::STORE_NONE: break;
           default: {
             LOG_ERROR("xattr add/update - add to state - ProvOpStoreType not handled" << FileProvenanceConstants::provOpStoreTypeToStr(provType));
             std::stringstream cause;
             cause << "xattr add/update - add to state - ProvOpStoreType not handled" << FileProvenanceConstants::provOpStoreTypeToStr(provType);
             throw std::logic_error(cause.str());
           }
         }
         } break;
      }
      if (provType == FileProvenanceConstants::ProvOpStoreType::STORE_ALL) {
        std::string xattrOpVal = ElasticHelper::addXAttrOp(ElasticHelper::opId(row), row, xattr.mValue, mlAux.second, mlAux.first);
        bulkOps.push_back(xattrOpVal);
      }
    } break;
    case FileProvenanceConstants::Operation::OP_XATTR_DELETE: {
      FPXAttrBufferPK xattrBufferKey(row.mInodeId, FileProvenanceConstants::XATTRS_USER_NAMESPACE, row.mXAttrName, row.mLogicalTime);
      std::string xattrStateVal = ElasticHelper::deleteXAttrFromState(ElasticHelper::stateId(row), row);
      bulkOps.push_back(xattrStateVal);
      if (provType == FileProvenanceConstants::ProvOpStoreType::STORE_ALL) {
        std::string xattrOpVal = ElasticHelper::deleteXAttrOp(ElasticHelper::opId(row), row, mlAux.second, mlAux.first);
        bulkOps.push_back(xattrOpVal);
      }
    } break;
    default: {
      LOG_WARN("operation not implemented:" << row.mOperation);
      std::stringstream cause;
      cause << "operation not implemented:" << row.mOperation;
      throw std::logic_error(cause.str());
    }
  }
  //cleanup - nop
  switch (op) {
    case FileProvenanceConstants::Operation::OP_CREATE:
    case FileProvenanceConstants::Operation::OP_DELETE:
    case FileProvenanceConstants::Operation::OP_MODIFY_DATA:
    case FileProvenanceConstants::Operation::OP_ACCESS_DATA: {
      return boost::make_tuple("index", bulkOps, row.getPK(), boost::none);
    } break;
    case FileProvenanceConstants::Operation::OP_XATTR_ADD:
    case FileProvenanceConstants::Operation::OP_XATTR_UPDATE:
    case FileProvenanceConstants::Operation::OP_XATTR_DELETE: {
      FPXAttrBufferPK xattrBufferKey(row.mInodeId, FileProvenanceConstants::XATTRS_USER_NAMESPACE, row.mXAttrName, row.mLogicalTime);
      return boost::make_tuple("index", bulkOps, row.getPK(), xattrBufferKey);
    } break;
    default: {
      LOG_WARN("operation not stored:" << row.mOperation);
      std::stringstream cause;
      cause << "operation not stored:" << row.mOperation;
      throw std::logic_error(cause.str());;
    }
  }
}

FPXAttrBufferRow FileProvenanceElasticDataReader::readBufferedXAttr(FPXAttrBufferPK xattrBufferKey) {
  boost::optional<FPXAttrBufferRow> xAttrBufferVal = mXAttrBuffer.get(mNdbConnection, xattrBufferKey);
  if (!xAttrBufferVal) {
    LOG_WARN("no such xattr in buffer:" << xattrBufferKey.to_string());
    std::stringstream cause;
    cause << "no such xattr in buffer: " << xattrBufferKey.to_string();
    throw std::logic_error(cause.str());;
  }
  return xAttrBufferVal.get();
}

boost::optional<FileProvenanceConstants::ProvOpStoreType> FileProvenanceElasticDataReader::datasetProvCore(Int64 datasetId) {
  FPXAttrVersionsK provTypeKey(datasetId, FileProvenanceConstants::XATTRS_USER_NAMESPACE, FileProvenanceConstants::PROV_CORE);
  boost::optional<FPXAttrBufferRow> provCore = getProvCore(provTypeKey);
  if(provCore) {
    return FileProvenanceConstants::provCore(provCore.get().mValue).first;
  } else {
    LOG_WARN("no prov type detected - dataset:" << datasetId);
    return boost::none;
  }
}

boost::optional<FPXAttrBufferRow> FileProvenanceElasticDataReader::getProvCore(FPXAttrVersionsK versionsKey) {
  std::vector<FPXAttrBufferRow> xattrVersions = mXAttrBuffer.get(mNdbConnection, versionsKey);
  std::sort(xattrVersions.begin(), xattrVersions.end(),
      [](FPXAttrBufferRow a, FPXAttrBufferRow b) {return a.mInodeLogicalTime > b.mInodeLogicalTime; });
  if(xattrVersions.size() == 0) {
    LOG_WARN("no xattr for key:" << versionsKey.to_string());
    return boost::none;
  }
  return xattrVersions.front();
}

FileProvenanceElasticDataReader::~FileProvenanceElasticDataReader() {
  
}