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
 * File:   AppProvenanceElasticDataReader.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef APPPROVENANCEELASTICDATAREADER_H
#define APPPROVENANCEELASTICDATAREADER_H

#include "NdbDataReaders.h"
#include "AppProvenanceTableTailer.h"
#include "boost/optional.hpp"
#include "boost/date_time.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

class AppProvenanceElasticDataReader : public NdbDataReader<AppProvenanceRow, SConn, AppPKeys> {
public:
  AppProvenanceElasticDataReader(SConn connection, const bool hopsworks);
  virtual ~AppProvenanceElasticDataReader();
private:
  void processAddedandDeleted(AppPq* data_batch, Bulk<AppPKeys>& bulk);
  string bulk_add_json(AppProvenanceRow row);
  string readable_timestamp(Int64 timestamp);
};

class AppProvenanceElasticDataReaders :  public NdbDataReaders<AppProvenanceRow, SConn, AppPKeys>{
  public:
    AppProvenanceElasticDataReaders(SConn* connections, int num_readers,const bool hopsworks,
          TimedRestBatcher<AppPKeys>* restEndpoint) : 
    NdbDataReaders(restEndpoint){
      for(int i=0; i<num_readers; i++){
        AppProvenanceElasticDataReader* dr = new AppProvenanceElasticDataReader(connections[i], hopsworks);
        dr->start(i, this);
        mDataReaders.push_back(dr);
      }
    }
};

#endif /* APPPROVENANCEELASTICDATAREADER_H */

