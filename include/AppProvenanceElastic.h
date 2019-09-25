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

#ifndef APPPROVENANCEELASTIC_H
#define APPPROVENANCEELASTIC_H

#include "ElasticSearchWithMetrics.h"
#include "AppProvenanceTableTailer.h"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>

class AppProvenanceElastic : public ElasticSearchWithMetrics {
public:
  AppProvenanceElastic(std::string elastic_addr, std::string index,
          int time_to_wait_before_inserting, int bulk_size,
          const bool stats, SConn conn);

  virtual ~AppProvenanceElastic();
private:
  const std::string mIndex;
  std::string mElasticBulkAddr;
  SConn mConn;

  virtual void process(std::vector<eBulk>* bulks);
  bool bulkRequest(eEvent& event);
};

#endif /* APPPROVENANCEELASTIC_H */