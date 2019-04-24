/*
 * Copyright (C) 2016 Hops.io
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
 * File:   AppProvenanceElastic.h
 * Author: Alexandru Ormenisan <aaor@kth.se>
 *
 * Created on June 9, 2016, 1:39 PM
 */

#ifndef APPPROVENANCEELASTIC_H
#define APPPROVENANCEELASTIC_H

#include "ElasticSearchBase.h"
#include "AppProvenanceTableTailer.h"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>

namespace bc = boost::accumulators;

typedef bc::accumulator_set<double, bc::stats<bc::tag::mean, bc::tag::min, bc::tag::max> > Accumulator;

typedef Bulk<AppPKeys> AppPBulk;

class AppProvenanceElastic : public ElasticSearchBase<AppPKeys> {
public:
  AppProvenanceElastic(string elastic_addr, string index,
          int time_to_wait_before_inserting, int bulk_size,
          const bool stats, SConn conn);

  bool addDoc(Int64 inodeId, string json);
  bool addBulk(string json);
  bool deleteDocsByQuery(string json);
  bool deleteSchemaForINode(Int64 inodeId, string json);

  virtual ~AppProvenanceElastic();
private:
  const string mIndex;
  const bool mStats;

  string mElasticBulkAddr;

  SConn sConn;

  Accumulator mBatchingAcc;
  Accumulator mWaitTimeBeforeProcessingAcc;
  Accumulator mProcessingAcc;
  Accumulator mWaitTimeUntillElasticCalledAcc;
  Accumulator mTotalTimePerEventAcc;
  Accumulator mTotalTimePerBulkAcc;
  long mTotalNumOfEventsProcessed;
  long mTotalNumOfBulksProcessed;
  ptime mFirstEventArrived;
  bool mIsFirstEventArrived;

  virtual void process(vector<AppPBulk>* bulks);

  void stats(vector<AppPBulk>* bulks);
  void stats(AppPBulk bulk, ptime t_elastic_done);
};

#endif /* APPPROVENANCEELASTIC_H */