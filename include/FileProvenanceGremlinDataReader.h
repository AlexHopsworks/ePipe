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
 * File:   FileProvenanceGremlinDataReader.h
 * Author: Alexandru Ormenisan <aaor@kth.se>
 *
 */

#ifndef FILEPROVENANCEGREMLINDATAREADER_H
#define FILEPROVENANCEGREMLINDATAREADER_H

#include "NdbDataReaders.h"
#include "FileProvenanceJanusGraph.h"
#include "boost/optional.hpp"
#include "boost/date_time.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"
#include "tables/INodeTable.h"

class FileProvenanceGremlinDataReader : public NdbDataReader<FileProvenanceRow, SConn, PKeys> {
public:
  FileProvenanceGremlinDataReader(SConn inodeConnection, const bool hopsworks, const int lru_cap);
  virtual ~FileProvenanceGremlinDataReader();
private:
  INodeTable mInodesTable;
  virtual void processAddedandDeleted(Pq* data_batch, PBulk& bulk);
  virtual string opBindings(FileProvenanceRow row);
  virtual void isFeatureGroup(FileProvenanceRow row, string projectName, string datasetName);
};
class FileProvenanceGremlinDataReaders :  public NdbDataReaders<FileProvenanceRow, SConn, PKeys>{
  public:
    FileProvenanceGremlinDataReaders(SConn* connections, int num_readers,const bool hopsworks, FileProvenanceJanusGraph* janusGraph, const int lru_cap) : 
    NdbDataReaders(janusGraph){
      for(int i=0; i<num_readers; i++){
        FileProvenanceGremlinDataReader* dr = new FileProvenanceGremlinDataReader(connections[i], hopsworks, lru_cap);
        dr->start(i, this);
        mDataReaders.push_back(dr);
      }
    }
};

#endif /* FILEPROVENANCEGREMLINDATAREADER_H */