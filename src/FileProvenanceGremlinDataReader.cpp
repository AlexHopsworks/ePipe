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
 * File:   FileProvenanceGremlinDataReader.cpp
 * Author: Alexandru Ormenisan <aaor@kth.se>
 * 
 */

#include "FileProvenanceGremlinDataReader.h"

FileProvenanceGremlinDataReader::FileProvenanceGremlinDataReader(SConn inodeConnection, const bool hopsworks, const int lru_cap)
: NdbDataReader(inodeConnection, hopsworks), mInodesTable(lru_cap) {
}

void FileProvenanceGremlinDataReader::processAddedandDeleted(Pq* data_batch, PBulk& bulk) {
  INodeMap inodes = mInodesTable.get(mNdbConnection, data_batch);

  rapidjson::StringBuffer sbOp;
  rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

  opWriter.StartObject();

  opWriter.Key("gremlin");
  opWriter.String("batchFileOps(batch);");

  opWriter.Key("language");
  opWriter.String("gremlin-groovy");

  opWriter.Key("bindings");
  opWriter.StartObject();
  
  opWriter.Key("batch");
  opWriter.StartArray();

  vector<ptime> arrivalTimes(data_batch->size());
  int i = 0;
  for (Pq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
    FileProvenanceRow row = *it;
    arrivalTimes[i] = row.mEventCreationTime;

    if (inodes.find(row.mProjectId) == inodes.end()) {
      LOG_INFO("no project");
      continue;
    }
    string projectName = inodes[row.mProjectId].mName;

    if (inodes.find(row.mDatasetId) == inodes.end()) {
      LOG_INFO("no dataset");
      continue;
    }
    string datasetName = inodes[row.mDatasetId].mName;
    isFeatureGroup(row, projectName, datasetName);

    FileProvenancePK rowPK = row.getPK();
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

void FileProvenanceGremlinDataReader::isFeatureGroup(FileProvenanceRow row, string projectName, string datasetName) {
  stringstream aux; 
  aux << projectName << "_featurestore.db";
  string featurestore = aux.str();
  if(datasetName == featurestore) {
    LOG_INFO("accessing feature store: ");
  }
}
string FileProvenanceGremlinDataReader::opBindings(FileProvenanceRow row) {
  //time
  using namespace boost::posix_time;
  using namespace boost::gregorian;
  time_t raw_t = (time_t)row.mTimestamp/1000; //time_t is time in seconds?
  ptime timestamp = from_time_t(raw_t);
  stringstream t_date;
  t_date << timestamp.date().year() << "." << timestamp.date().month() << "." << timestamp.date().day();
  long t_hour = timestamp.time_of_day().hours();
  stringstream t;
  t << timestamp.time_of_day().hours() << ":" << timestamp.time_of_day().minutes() << ":" << timestamp.time_of_day().seconds();

  rapidjson::StringBuffer sbOp;
  rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);
  opWriter.StartObject();

  opWriter.Key("i_o_id");
  opWriter.String(row.getPK().to_string().c_str());

  opWriter.Key("i_o_name");
  opWriter.String(row.mOperation.c_str());

  opWriter.Key("i_id");
  opWriter.Int64(row.mInodeId);

  opWriter.Key("p_i_id");
  opWriter.Int64(row.mParentId);

  opWriter.Key("p_id");
  opWriter.Int64(row.mProjectId);

  opWriter.Key("d_id");
  opWriter.Int64(row.mDatasetId);

  opWriter.Key("u_id");
  opWriter.Int(row.mUserId);

  opWriter.Key("i_name");
  opWriter.String(row.mInodeName.c_str());

  opWriter.Key("t");
  opWriter.String(t.str().c_str());

  opWriter.Key("t_hour");
  opWriter.Int64(t_hour);

  opWriter.Key("t_date");
  opWriter.String(t_date.str().c_str());

  opWriter.EndObject();

  stringstream out;
  out << sbOp.GetString() << endl;
  return out.str();
}

FileProvenanceGremlinDataReader::~FileProvenanceGremlinDataReader() {
  
}

