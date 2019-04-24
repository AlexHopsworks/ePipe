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
 * File:   AppProvenanceElasticDataReader.cpp
 * Author: Mahmoud Ismail <maism@kth.se>
 * 
 */

#include "AppProvenanceElasticDataReader.h"

AppProvenanceElasticDataReader::AppProvenanceElasticDataReader(SConn connection, const bool hopsworks)
: NdbDataReader(connection, hopsworks) {
}

void AppProvenanceElasticDataReader::processAddedandDeleted(AppPq* data_batch, Bulk<AppPKeys>& bulk) {
  vector<ptime> arrivalTimes(data_batch->size());
  stringstream out;
  int i = 0;
  for (AppPq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
    AppProvenanceRow row = *it;
    arrivalTimes[i] = row.mEventCreationTime;
    AppProvenancePK rowPK = row.getPK();
    bulk.mPKs.push_back(rowPK);
    out << bulk_add_json(row) << endl;
  }

  bulk.mArrivalTimes = arrivalTimes;
  bulk.mJSON = out.str();
}

string AppProvenanceElasticDataReader::bulk_add_json(AppProvenanceRow row) {
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

  dataVal.AddMember("app_id",     rapidjson::Value().SetString(row.mId.c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("app_state",  rapidjson::Value().SetString(row.mState.c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("timestamp",  rapidjson::Value().SetString(readable_timestamp(row.mTimestamp).c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("app_name",   rapidjson::Value().SetString(row.mName.c_str(), dataAlloc), dataAlloc);
  dataVal.AddMember("app_user",   rapidjson::Value().SetString(row.mUser.c_str(), dataAlloc), dataAlloc);

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

string AppProvenanceElasticDataReader::readable_timestamp(Int64 timestamp) {
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

AppProvenanceElasticDataReader::~AppProvenanceElasticDataReader() {
  
}