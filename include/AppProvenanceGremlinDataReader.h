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
 * File:   APPProvenanceGremlinDataReader.h
 * Author: Alexandru Ormenisan <aaor@kth.se>
 *
 */

#ifndef APPPROVENANCEGREMLINDATAREADER_H
#define APPPROVENANCEGREMLINDATAREADER_H

#include "NdbDataReaders.h"
#include "AppProvenanceJanusGraph.h"
#include "boost/optional.hpp"
#include "boost/date_time.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

class AppProvenanceGremlinDataReader : public NdbDataReader<AppProvenanceRow, SConn, AppPKeys> {
public:
  AppProvenanceGremlinDataReader(SConn connection, const bool hopsworks);
  virtual ~AppProvenanceGremlinDataReader();
private:

  virtual void processAddedandDeleted(AppPq* data_batch, AppPBulk& bulk);
  virtual string opBindings(AppProvenanceRow row);
};
class AppProvenanceGremlinDataReaders :  public NdbDataReaders<AppProvenanceRow, SConn, AppPKeys>{
  public:
    AppProvenanceGremlinDataReaders(SConn* connections, int num_readers,const bool hopsworks, AppProvenanceJanusGraph* janusGraph) : 
    NdbDataReaders(janusGraph){
      for(int i=0; i<num_readers; i++){
        AppProvenanceGremlinDataReader* dr = new AppProvenanceGremlinDataReader(connections[i], hopsworks);
        dr->start(i, this);
        mDataReaders.push_back(dr);
      }
    }
};

#endif /* APPPROVENANCEGREMLINDATAREADER_H */