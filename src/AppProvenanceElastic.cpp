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

#include "AppProvenanceElastic.h"

using namespace Utils;

AppProvenanceElastic::AppProvenanceElastic(std::string elastic_addr, std::string index,
        int time_to_wait_before_inserting, int bulk_size, const bool stats, SConn conn) : 
ElasticSearchBase(elastic_addr, time_to_wait_before_inserting, bulk_size),
mIndex(index), mStats(stats), sConn(conn), mStartTime(getCurrentTime()) {
  mElasticBulkAddr = getElasticSearchBulkUrl(mIndex);
}

void AppProvenanceElastic::process(std::vector<AppPBulk>* bulks) {
  AppPKeys mAppProvLogKs;

  std::string batch;
  for (std::vector<AppPBulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
    AppPBulk bulk = *it;
    batch.append(bulk.mJSON);
    mAppProvLogKs.insert(mAppProvLogKs.end(), bulk.mPKs.mAppProvLogKs.begin(), bulk.mPKs.mAppProvLogKs.end());
  }

  //TODO: handle failures
  if (httpPostRequest(mElasticBulkAddr, batch)) {
    if (!mAppProvLogKs.empty()) {
      AppProvenanceLogTable().removeLogs(sConn, mAppProvLogKs);
    }
    if (mStats) {
      stats(bulks);
    }
  }else{
    if(mStats){
      mCounters.check();
      mCounters.elasticSearchRequestFailed();
    }
  }
}

void AppProvenanceElastic::stats(std::vector<AppPBulk>* bulks) {
  ptime t_end = getCurrentTime();

  ptime firstEventInCurrentBulksArrivalTime = bulks->at(0).mArrivalTimes.at(0);
  int numOfEvents = 0;
  for (std::vector<AppPBulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
    AppPBulk bulk = *it;
    stats(bulk, t_end);
    numOfEvents += bulk.mArrivalTimes.size();
  }

  float bulksTotalTime = getTimeDiffInMilliseconds(firstEventInCurrentBulksArrivalTime, t_end);
  float bulksEventPerSecond = (numOfEvents * 1000.0) / bulksTotalTime;

  LOG_INFO("Bulks[" << numOfEvents << "/" << bulks->size() << "] took " << bulksTotalTime << " msec at Rate=" << bulksEventPerSecond << " events/second");
}

void AppProvenanceElastic::stats(AppPBulk bulk, ptime t_elastic_done) {
  mCounters.check();

  int size = bulk.mArrivalTimes.size();
  float batch_time, wait_time, processing_time, ewait_time, total_time;
  if (size > 0) {
    batch_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[0], bulk.mArrivalTimes[size - 1]);
    wait_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[size - 1], bulk.mStartProcessing);
    total_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[0], t_elastic_done);
  }

  processing_time = getTimeDiffInMilliseconds(bulk.mStartProcessing, bulk.mEndProcessing);
  ewait_time = getTimeDiffInMilliseconds(bulk.mEndProcessing, t_elastic_done);

  for (int i = 0; i < size; i++) {
    float total_time_per_event = getTimeDiffInMilliseconds(bulk.mArrivalTimes[i], t_elastic_done);
    mCounters.addTotalTimePerEvent(total_time_per_event);
  }

  mCounters.processBulk(batch_time, wait_time, processing_time,
      ewait_time, total_time, size);
}

AppProvenanceElastic::~AppProvenanceElastic() {
}

std::string AppProvenanceElastic::getMetrics() const {
  std::stringstream out;
  out << "up_seconds " << getTimeDiffInSeconds(mStartTime, getCurrentTime())
  << std::endl;
  out << mCounters.getMetrics(mStartTime);
  return out.str();
}
