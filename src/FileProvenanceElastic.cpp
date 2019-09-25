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

#include <FileProvenanceConstants.h>
#include "FileProvenanceElastic.h"

FileProvenanceElastic::FileProvenanceElastic(std::string elastic_addr, std::string index,
        int time_to_wait_before_inserting, int bulk_size, const bool stats, SConn conn) : 
ElasticSearchWithMetrics(elastic_addr, time_to_wait_before_inserting, bulk_size, stats),
mIndex(index), mConn(conn) {
  mElasticBulkAddr = getElasticSearchBulkUrl(mIndex);
}

void FileProvenanceElastic::process(std::vector<eBulk>* bulks) {
  std::vector<const LogHandler*> logRHandlers;
  std::string batch;
  for (auto it = bulks->begin(); it != bulks->end();++it) {
    eBulk bulk = *it;
    std::string out;
    for(auto e : bulk.mEvents){
      if(e.getJSON() != FileProvenanceConstants::ELASTIC_NOP) {
        out += e.getJSON();
      }
    }
    batch += out;
    logRHandlers.insert(logRHandlers.end(), bulk.mLogHandlers.begin(), bulk.mLogHandlers.end());
    if(mStats){
      mCounters.bulkReceived(bulk);
    }
  }

  ptime start_time = Utils::getCurrentTime();
  if (httpPostRequest(mElasticBulkAddr, batch)) {
    FileProvenanceLogTable().cleanLogs(mConn, logRHandlers);
    if (mStats) {
      mCounters.bulksProcessed(start_time, bulks);
    }
  }else{
    for (auto it = bulks->begin(); it != bulks->end();++it) {
      eBulk bulk = *it;
      for(eEvent event : bulk.mEvents){
        if(!bulkRequest(event)){
          LOG_FATAL("Failure while processing log : "
                        << event.getLogHandler()->getDescription() << std::endl << event.getJSON());
        }
      }
      if (mStats) {
        mCounters.bulkProcessed(start_time, bulk);
      }
    }
  }
}

bool FileProvenanceElastic::bulkRequest(eEvent& event) {
  if (httpPostRequest(mElasticBulkAddr, event.getJSON())){
    FileProvenanceLogTable().cleanLog(mConn, event.getLogHandler());
    return true;
  }
  return false;
}


FileProvenanceElastic::~FileProvenanceElastic() {
}