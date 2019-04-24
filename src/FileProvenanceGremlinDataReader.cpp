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

FileProvenanceGremlinDataReader::FileProvenanceGremlinDataReader(SConn connection, const bool hopsworks)
: NdbDataReader(connection, hopsworks) {
}

void FileProvenanceGremlinDataReader::processAddedandDeleted(Pq* data_batch, PBulk& bulk) {
  LOG_ERROR("file provenance json 1");
  rapidjson::Document document;
  document.SetObject();
  rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
  
  document.AddMember("gremlin", "batchFileOps(batch);", allocator);
  document.AddMember("language", "gremlin-groovy", allocator);
  rapidjson::Value bindings(rapidjson::kObjectType);
  rapidjson::Value batchBinding(rapidjson::kArrayType);
  vector<ptime> arrivalTimes(data_batch->size());
  int i = 0;
  LOG_ERROR("file provenance json 2");
  for (Pq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
    LOG_ERROR("file provenance json repeat 3");
    FileProvenanceRow row = *it;
    arrivalTimes[i] = row.mEventCreationTime;
    FileProvenancePK rowPK = row.getPK();
    bulk.mPKs.push_back(rowPK);

    using namespace boost::posix_time;
    using namespace boost::gregorian;
    time_t raw_t = (time_t)row.mTimestamp/1000; //time_t is time in seconds?
    ptime timestamp = from_time_t(raw_t);
    stringstream t_date;
    t_date << timestamp.date().year() << "." << timestamp.date().month() << "." << timestamp.date().day();
    int t_hour = timestamp.time_of_day().hours();
    stringstream t;
    t << timestamp.time_of_day().hours() << ":" << timestamp.time_of_day().minutes() << ":" << timestamp.time_of_day().seconds();
    LOG_ERROR("file provenance json repeat 3 1");
    rapidjson::Value binding(rapidjson::kObjectType);
    LOG_ERROR("file provenance json repeat 3 2");
    LOG_ERROR("file provenance json:" << row.getPK().to_string().c_str());
    binding.AddMember("i_o_id", rapidjson::Value().SetString(row.getPK().to_string().c_str(), allocator), allocator);
    binding.AddMember("i_o_name", rapidjson::Value().SetString(row.mOperation.c_str(), allocator), allocator);
    binding.AddMember("i_id", rapidjson::Value().SetString(std::to_string(row.mInodeId).c_str(), allocator), allocator);
    binding.AddMember("p_i_id", rapidjson::Value().SetString(std::to_string(row.mParentId).c_str(), allocator), allocator);
    binding.AddMember("p_id", rapidjson::Value().SetString(std::to_string(row.mProjectId).c_str(), allocator), allocator);
    binding.AddMember("d_id", rapidjson::Value().SetString(std::to_string(row.mDatasetId).c_str(), allocator), allocator);
    binding.AddMember("u_id", rapidjson::Value().SetString(std::to_string(row.mUserId).c_str(), allocator), allocator);
    binding.AddMember("i_name", rapidjson::Value().SetString(row.mInodeName.c_str(), allocator), allocator);
    binding.AddMember("t", rapidjson::Value().SetString(t.str().c_str(), allocator), allocator);
    binding.AddMember("t_hour", rapidjson::Value().SetString(std::to_string(t_hour).c_str(), allocator), allocator);
    binding.AddMember("t_date", rapidjson::Value().SetString(t_date.str().c_str(), allocator), allocator);
    LOG_ERROR("file provenance json repeat 3 4");
    batchBinding.PushBack(binding, allocator);

    LOG_ERROR("file provenance json repeat 4");
  }
  bindings.AddMember("batch", batchBinding, allocator);
  document.AddMember("bindings", bindings, allocator);
  LOG_ERROR("file provenance json 5");
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  document.Accept(writer);
  stringstream out;
  out << buffer.GetString() << endl;
  LOG_ERROR("file provenance json:" << out.str());
  bulk.mArrivalTimes = arrivalTimes;
  bulk.mJSON = out.str();
}

rapidjson::Value& FileProvenanceGremlinDataReader::fileOpBindings(FileProvenanceRow row, rapidjson::Document::AllocatorType& allocator) {
  //time
  using namespace boost::posix_time;
  using namespace boost::gregorian;
  time_t raw_t = (time_t)row.mTimestamp/1000; //time_t is time in seconds?
  ptime timestamp = from_time_t(raw_t);
  stringstream t_date;
  t_date << timestamp.date().year() << "." << timestamp.date().month() << "." << timestamp.date().day();
  int t_hour = timestamp.time_of_day().hours();
  stringstream t;
  t << timestamp.time_of_day().hours() << ":" << timestamp.time_of_day().minutes() << ":" << timestamp.time_of_day().seconds();
  LOG_ERROR("file provenance json repeat 3 1");
  rapidjson::Value binding(rapidjson::kObjectType);
  LOG_ERROR("file provenance json repeat 3 2");
  binding.AddMember("i_o_id", rapidjson::StringRef(row.getPK().to_string().c_str()), allocator);
  LOG_ERROR("file provenance json repeat 3 3");
  binding.AddMember("i_o_name", rapidjson::StringRef(row.mOperation.c_str()), allocator);
  binding.AddMember("i_id", rapidjson::Value().SetInt64(row.mInodeId), allocator);
  binding.AddMember("p_i_id", rapidjson::Value().SetInt64(row.mParentId), allocator);
  binding.AddMember("p_id", rapidjson::Value().SetInt64(row.mProjectId), allocator);
  binding.AddMember("d_id", rapidjson::Value().SetInt64(row.mDatasetId), allocator);
  binding.AddMember("u_id", rapidjson::Value().SetInt(row.mUserId), allocator);
  binding.AddMember("i_name", rapidjson::StringRef(row.mInodeName.c_str()), allocator);
  binding.AddMember("t", rapidjson::StringRef(t.str().c_str()), allocator);
  binding.AddMember("t_hour", rapidjson::Value().SetInt(t_hour), allocator);
  binding.AddMember("t_date", rapidjson::StringRef(t_date.str().c_str()), allocator);
  LOG_ERROR("file provenance json repeat 3 4");
  return binding;
}

FileProvenanceGremlinDataReader::~FileProvenanceGremlinDataReader() {
  
}

