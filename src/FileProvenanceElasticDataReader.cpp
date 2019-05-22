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

  static string add(FileProvenanceRow row, string mlId, string mlType) {
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
    dataVal.AddMember("io_user_id",          rapidjson::Value().SetInt(row.mUserId), dataAlloc);
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

  static string update(string key, string name, string val) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(key.c_str(), opAlloc), opAlloc);

    op.AddMember("update", opVal, opAlloc);

    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);

    rapidjson::Value rname(name.c_str(), dataAlloc);
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

};

class XAttrBufferReader {
public:
  XAttrBufferReader(SConn conn) : mConn(conn) {
  }

  boost::optional<XAttrRow> getXAttr(XAttrPK key) {
    boost::optional<XAttrRow> row = mXAttr.get(mConn, key);
    if(row) {
      LOG_INFO("retrieved xattr:" << key.mName);
    } else {
      return  boost::none;
    }
    return row;
  }

  string getMLId(FileProvenanceRow row) {
    boost::optional<XAttrRow> mlIdXAttr = getXAttr(XAttrPK(row.mInodeId, 
      FileProvenanceConstants::XATTRS_USER_NAMESPACE, FileProvenanceConstants::XATTRS_ML_ID));
    string ml_id;
    if(mlIdXAttr) {
      ml_id = parseMLId(mlIdXAttr.get());
    } else {
      ml_id = "no_such_xattr";
    } 
    return ml_id;
  }

  string parseMLId(XAttrRow mlIdXAttr) {
    rapidjson::Document d;
    d.Parse(mlIdXAttr.mValue.c_str());
    rapidjson::Value& spaceId = d[FileProvenanceConstants::ML_ID_SPACE.c_str()];
    rapidjson::Value& base = d[FileProvenanceConstants::ML_ID_BASE.c_str()];
    rapidjson::Value& version = d[FileProvenanceConstants::ML_ID_VERSION.c_str()];
    stringstream ml_id;
    ml_id << spaceId.GetString() << "_" << base.GetString() << "_" << version.GetString();
    return ml_id.str();
  }

  string getMLDeps(FileProvenanceRow row) {
    boost::optional<XAttrRow> mlDepsXAttr = getXAttr(XAttrPK(row.mInodeId, 
      FileProvenanceConstants::XATTRS_USER_NAMESPACE, FileProvenanceConstants::H_XATTR_ML_DEPS));
    string ml_deps;
    if(mlDepsXAttr) {
      ml_deps = mlDepsXAttr.get().mValue;
    } else {
      ml_deps = "no_such_xattr";
    }
    return ml_deps;
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
    FileProvenancePK fpLogPK = row.getPK();
    boost::tuple<string, boost::optional<XAttrPK> > result = process_row(row);
    boost::optional<XAttrPK> fpXAttrBufferPK = boost::get<1>(result);
    bulk.mPKs.mFileProvLogKs.push_back(fpLogPK);
    bulk.mPKs.mXAttrBufferKs.push_back(fpXAttrBufferPK);
    out << boost::get<0>(result) << endl;
  }
  bulk.mArrivalTimes = arrivalTimes;
  bulk.mJSON = out.str();
}

boost::tuple<string, boost::optional<XAttrPK> > FileProvenanceElasticDataReader::process_row(FileProvenanceRow row) {
    LOG_INFO("reading provenance for inode:" << row.mInodeId);
    if(row.mOperation == FileProvenanceConstants::H_OP_XATTR_ADD) {
      XAttrBufferReader reader(mNdbConnection);
      XAttrPK xattrBufferKey(row.mInodeId, 0, row.mXAttrName);
      string val;
      if(row.mXAttrName == FileProvenanceConstants::H_XATTR_ML_ID) {
        val = ElasticHelper::update(row.getPK().to_string(), "ml_id", reader.getMLId(row));
      } else if(row.mXAttrName == FileProvenanceConstants::H_XATTR_ML_DEPS) {
        val = ElasticHelper::update(row.getPK().to_string(), "ml_deps", reader.getMLDeps(row));
      } else {
        stringstream cause;
        cause << "xattr not handled: " << row.mXAttrName;
        throw cause.str();
      }
      return boost::make_tuple(val, xattrBufferKey);
    } else {
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
      } else if(FileProvenanceConstants::partOfMLModel(row)) {
        LOG_INFO("mlType: model part");
        mlType = FileProvenanceConstants::ML_TYPE_MODEL_PART;
        mlId = FileProvenanceConstants::getMLModelId(row);
      } else if(FileProvenanceConstants::partOfMLTDataset(row)) {
        LOG_INFO("mlType: dataset part");
        mlType = FileProvenanceConstants::ML_TYPE_TDATASET_PART;
        mlId = FileProvenanceConstants::getMLTDatasetId(row);
      } else if(FileProvenanceConstants::partOfMLFeature(row)) {
        LOG_INFO("mlType: feature part");
        mlType = FileProvenanceConstants::ML_TYPE_FEATURE_PART;
        mlId = FileProvenanceConstants::getMLFeatureId(row);
      } else {
        LOG_INFO("mlType: none");
      }
      return boost::make_tuple(ElasticHelper::add(row, mlId, mlType), boost::none);
    }
  }

FileProvenanceElasticDataReader::~FileProvenanceElasticDataReader() {
  
}