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
 * File:   FileProvenanceDataReader.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef FILEPROVENANCEDATAREADER_H
#define FILEPROVENANCEDATAREADER_H

#include "NdbDataReaders.h"
#include "FileProvenanceTableTailer.h"
#include "TimedRestBatcher.h"
// #include "ProvenanceElasticSearch.h"
// #include "FileProvenanceJanusGraph.h"
#include "boost/optional.hpp"

class FileProvenanceDataReader : public NdbDataReader<FileProvenanceRow, SConn, PKeys> {
public:
  FileProvenanceDataReader(SConn connection, const bool hopsworks);
  virtual ~FileProvenanceDataReader();
private:
  virtual void processAddedandDeleted(Pq* data_batch, Bulk<PKeys>& bulk);
};

class ProvenanceDataReaders :  public NdbDataReaders<FileProvenanceRow, SConn, PKeys>{
  public:
    ProvenanceDataReaders(SConn* connections, int num_readers,const bool hopsworks,
          TimedRestBatcher<PKeys>* restEndpoint) : 
    NdbDataReaders(restEndpoint){
      for(int i=0; i<num_readers; i++){
        FileProvenanceDataReader* dr = new FileProvenanceDataReader(connections[i], hopsworks);
        dr->start(i, this);
        mDataReaders.push_back(dr);
      }
    }
};

#endif /* FILEPROVENANCEDATAREADER_H */

