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

#ifndef FILEPROVENANCEELASTIC_H
#define FILEPROVENANCEELASTIC_H

#include "ElasticSearchBase.h"
#include "FileProvenanceTableTailer.h"
#include "tables/FileProvenanceXAttrBufferTable.h"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>

namespace bc = boost::accumulators;

typedef bc::accumulator_set<double, bc::stats<bc::tag::mean> > Accumulator;

struct ProvKeys {
  std::string mIndex;
  PKeys mFileProvLogKs;
  FPXAttrBKeys mXAttrBufferKs;
};
typedef Bulk<ProvKeys> PBulk;

struct FileMovingCounters{
  virtual void check() = 0;
  virtual void addTotalTimePerEvent(float total_time_per_event) = 0;
  virtual void processBulk(float batch_time, float wait_time, float processing_time,
      float ewait_time, float total_time, int size) = 0;
  virtual void elasticSearchRequestFailed() = 0;
  virtual std::string getMetrics(const ptime startTime) const = 0;
};

struct FileMovingCountersImpl : public FileMovingCounters{

  FileMovingCountersImpl(int stepInSeconds, std::string prefix) : mStepSeconds
  (stepInSeconds), mPrefix(prefix){
    mlastCleared = getCurrentTime();
    reset();
  }

  void check() override{
    if(mStepSeconds == -1)
      return;

    ptime now = getCurrentTime();
    if(getTimeDiffInSeconds(mlastCleared, now) >= mStepSeconds){
      reset();
      mlastCleared = now;
    }
  }

  void addTotalTimePerEvent(float total_time_per_event) override{
    mTotalTimePerEventAcc(total_time_per_event);
  }
  void processBulk(float batch_time, float wait_time, float processing_time,
      float ewait_time, float total_time, int size) override{
    mBatchingAcc(batch_time);
    mWaitTimeBeforeProcessingAcc(wait_time);
    mProcessingAcc(processing_time);
    mWaitTimeUntillElasticCalledAcc(ewait_time);
    mTotalTimePerBulkAcc(total_time);
    mTotalNumOfEventsProcessed += size;
    mTotalNumOfBulksProcessed++;
  }

  void elasticSearchRequestFailed() override {
    mElasticSearchFailedRequests++;
  }

  std::string getMetrics(const ptime startTime) const override{
    std::stringstream out;
    out << "relative_start_time_seconds{scope=\"" << mPrefix << "\"} " <<
       ( mStepSeconds == -1 || mTotalNumOfEventsProcessed == 0 ? 0 :
       getTimeDiffInSeconds(startTime,mlastCleared))<< std::endl;
    out << "num_processed_events{scope=\"" << mPrefix << "\"} " <<
    mTotalNumOfEventsProcessed << std::endl;
    out << "num_processed_batches{scope=\"" << mPrefix << "\"} " <<
    mTotalNumOfBulksProcessed << std::endl;
    if(mTotalNumOfEventsProcessed > 0 ) {
      out << "avg_total_time_per_event_milliseconds{scope=\"" << mPrefix <<
      "\"} " << bc::mean(mTotalTimePerEventAcc) << std::endl;
      out << "avg_total_time_per_batch_milliseconds{scope=\"" << mPrefix <<
      "\"} " << bc::mean(mTotalTimePerEventAcc) << std::endl;
      out << "avg_batching_time_milliseconds{scope=\"" << mPrefix << "\"} " <<
          bc::mean(mBatchingAcc) << std::endl;
      out << "avg_ndb_processing_time_milliseconds{scope=\"" << mPrefix <<
      "\"} " << bc::mean(mProcessingAcc) << std::endl;
      out << "avg_elastic_batching_time_milliseconds{scope=\"" << mPrefix <<
      "\"} " << bc::mean(mWaitTimeUntillElasticCalledAcc) << std::endl;
    }else{
      out << "avg_total_time_per_event_milliseconds{scope=\"" << mPrefix <<
      "\"} 0" << std::endl;
      out << "avg_total_time_per_batch_milliseconds{scope=\"" << mPrefix <<
      "\"} 0" << std::endl;
      out << "avg_batching_time_milliseconds{scope=\"" << mPrefix << "\"} 0"
      << std::endl;
      out << "avg_ndb_processing_time_milliseconds{scope=\"" << mPrefix <<
      "\"} 0" << std::endl;
      out << "avg_elastic_batching_time_milliseconds{scope=\"" << mPrefix <<
      "\"} 0" << std::endl;
    }

    out << "num_failed_elasticsearch_batch_requests{scope=\"" << mPrefix <<
    "\"} " << mElasticSearchFailedRequests << std::endl;
   return out.str();
  }
  private:
  int mStepSeconds;
  std::string mPrefix;

  ptime mlastCleared;
  Accumulator mBatchingAcc;
  Accumulator mWaitTimeBeforeProcessingAcc;
  Accumulator mProcessingAcc;
  Accumulator mWaitTimeUntillElasticCalledAcc;
  Accumulator mTotalTimePerEventAcc;
  Accumulator mTotalTimePerBulkAcc;
  Int64 mTotalNumOfEventsProcessed;
  Int64 mTotalNumOfBulksProcessed;
  Int64 mElasticSearchFailedRequests;

  void reset(){
    mBatchingAcc = {};
    mWaitTimeBeforeProcessingAcc = {};
    mProcessingAcc={};
    mWaitTimeUntillElasticCalledAcc={};
    mTotalTimePerEventAcc={};
    mTotalTimePerBulkAcc={};
    mTotalNumOfEventsProcessed = 0;
    mTotalNumOfBulksProcessed = 0;
    mElasticSearchFailedRequests = 0;
  }
};

struct FileMovingCountersSet : public FileMovingCounters{
  FileMovingCountersSet(){
    mCounters = {FileMovingCountersImpl(60, "last_minute"), FileMovingCountersImpl
                 (3600, "last_hour"), FileMovingCountersImpl(-1, "all_time")};
  }
  void check() override {
    for(auto& c : mCounters){
      c.check();
    }
  }

  void addTotalTimePerEvent(float total_time_per_event) override {
    for(auto& c : mCounters){
      c.addTotalTimePerEvent(total_time_per_event);
    }
  }

  void processBulk(float batch_time, float wait_time, float processing_time,
                   float ewait_time, float total_time, int size) override {
    for(auto& c : mCounters){
      c.processBulk(batch_time, wait_time, processing_time, ewait_time,
          total_time, size);
    }
  }

  void elasticSearchRequestFailed() override {
    for(auto& c : mCounters){
      c.elasticSearchRequestFailed();
    }
  }

  std::string getMetrics(const ptime startTime) const override {
    std::stringstream out;
    for(auto& c : mCounters){
      out << c.getMetrics(startTime);
    }
    return out.str();
  }

private:
  std::vector<FileMovingCountersImpl> mCounters;
};


class FileProvenanceElastic : public ElasticSearchBase<ProvKeys> {
public:
  FileProvenanceElastic(std::string elastic_addr, std::string index,
          int time_to_wait_before_inserting, int bulk_size,
          const bool stats, SConn conn);

  std::string getMetrics() const override;

  virtual ~FileProvenanceElastic();
private:
  const std::string mIndex;
  const bool mStats;
  std::string mElasticBulkAddr;
  SConn sConn;
  const ptime mStartTime;

  FileMovingCountersSet mCounters;

  virtual void process(std::vector<PBulk>* bulks);

  void stats(std::vector<PBulk>* bulks);
  void stats(PBulk bulk, ptime t_elastic_done);
};

#endif /* FILEPROVENANCEELASTIC_H */

