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

class Helper {
public:
  static std::list<boost::tuple<string, boost::optional<AppProvenancePK> > > process_row(AppProvenanceRow row) {
    LOG_INFO("app provenance:" << row.to_string());
    std::list<boost::tuple<string, boost::optional<AppProvenancePK> > > result;

    if(row.mFinishTime != 0) {
      AppProvenancePK finishId(row.mId, row.mState, row.mFinishTime);
      string finishLog = bulk_add_json(finishId, row.mName, row.mUser);
      result.push_back(boost::make_tuple(finishLog, boost::none));
    }
    AppProvenancePK runningId(row.mId, FileProvenanceConstants::APP_RUNNING_STATE, row.mStartTime);
    string runningLog = bulk_add_json(runningId, row.mName, row.mUser);
    result.push_back(boost::make_tuple(runningLog, boost::none));

    AppProvenancePK submitId(row.mId, FileProvenanceConstants::APP_SUBMITTED_STATE, row.mSubmitTime);
    string submitLog = bulk_add_json(submitId, row.mName, row.mUser);
    result.push_back(boost::make_tuple(submitLog, row.getPK()));

    return result;
  }

  static string bulk_add_json(AppProvenancePK key, string appName, string userName) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(key.to_string().c_str(), opAlloc), opAlloc);

    op.AddMember("update", opVal, opAlloc);

    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);

    string readable_t = readable_timestamp(key.mTimestamp);
    dataVal.AddMember("app_id",     rapidjson::Value().SetString(key.mId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("app_state",  rapidjson::Value().SetString(key.mState.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("timestamp",  rapidjson::Value().SetInt64(key.mTimestamp), dataAlloc);
    dataVal.AddMember("readable_timestamp",  rapidjson::Value().SetString(readable_t.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("app_name",   rapidjson::Value().SetString(appName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("app_user",   rapidjson::Value().SetString(userName.c_str(), dataAlloc), dataAlloc);

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

void AppProvenanceElasticDataReader::processAddedandDeleted(AppPq* data_batch, Bulk<AppPBulkKeys>& bulk) {
  vector<ptime> arrivalTimes(data_batch->size());
  stringstream out;
  int i = 0;
  for (AppPq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
    AppProvenanceRow row = *it;
    arrivalTimes[i] = row.mEventCreationTime;
    std::list<boost::tuple<string, boost::optional<AppProvenancePK> > > result = Helper::process_row(row);
    for(boost::tuple<string, boost::optional<AppProvenancePK> > item : result) {
      boost::optional<AppProvenancePK> apPK = boost::get<1>(item);
      bulk.mPKs.mAppProvLogKs.push_back(apPK);
      out << boost::get<0>(item) << endl;
    }
  }

  bulk.mArrivalTimes = arrivalTimes;
  bulk.mJSON = out.str();
}

AppProvenanceElasticDataReader::~AppProvenanceElasticDataReader() {
  
}