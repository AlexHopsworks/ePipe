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
 * File:   AppProvenanceGremlinDataReader.cpp
 * Author: Alexandru Ormenisan <aaor@kth.se>
 * 
 */

#include "AppProvenanceGremlinDataReader.h"

AppProvenanceGremlinDataReader::AppProvenanceGremlinDataReader(SConn connection, const bool hopsworks)
: NdbDataReader(connection, hopsworks) {
}

void AppProvenanceGremlinDataReader::processAddedandDeleted(AppPq* data_batch, AppPBulk& bulk) {
  rapidjson::StringBuffer sbOp;
  rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

  opWriter.StartObject();

  opWriter.Key("gremlin");
  opWriter.String("batchAppOps(batch);");

  opWriter.Key("language");
  opWriter.String("gremlin-groovy");

  opWriter.Key("bindings");
  opWriter.StartObject();
  
  opWriter.Key("batch");
  opWriter.StartArray();

  vector<ptime> arrivalTimes(data_batch->size());
  int i = 0;
  for (AppPq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
    AppProvenanceRow row = *it;
    arrivalTimes[i] = row.mEventCreationTime;
    AppProvenancePK rowPK = row.getPK();
    bulk.mPKs.push_back(rowPK);
    string opBinding = opBindings(row);
    opWriter.String(opBinding.c_str());
  }

  opWriter.EndArray();
  opWriter.EndObject();
  opWriter.EndObject();

  stringstream out;
  out << sbOp.GetString() << endl;

  bulk.mArrivalTimes = arrivalTimes;
  bulk.mJSON = out.str();
}

string AppProvenanceGremlinDataReader::opBindings(AppProvenanceRow row) {
  //time
  using namespace boost::posix_time;
  using namespace boost::gregorian;
  time_t raw_t = (time_t)row.mTimestamp/1000; //time_t is time in seconds?
  ptime timestamp = from_time_t(raw_t);
  stringstream date;
  date << timestamp.date().year() << "." << timestamp.date().month() << "." << timestamp.date().day();
  long hour = timestamp.time_of_day().hours();
  stringstream o_time;
  o_time << timestamp.time_of_day().hours() << ":" << timestamp.time_of_day().minutes() << ":" << timestamp.time_of_day().seconds();

  rapidjson::StringBuffer sbOp;
  rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);
  opWriter.StartObject();

  opWriter.Key("a_o_id");
  opWriter.String(row.getPK().to_string().c_str());

  opWriter.Key("a_state");
  opWriter.String(row.mState.c_str());

  opWriter.Key("a_name");
  opWriter.String(row.mName.c_str());

  opWriter.Key("u_id");
  opWriter.String(row.mUser.c_str());

  opWriter.Key("app_o_time");
  opWriter.String(o_time.str().c_str());

  

  opWriter.EndObject();

  stringstream out;
  out << sbOp.GetString() << endl;
  return out.str();
}

AppProvenanceGremlinDataReader::~AppProvenanceGremlinDataReader() {
  
}

