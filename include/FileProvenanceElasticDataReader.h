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
 * File:   FileProvenanceElasticDataReader.h
 * Author: Alexandru Ormenisan <aaor@kth.se>
 *
 */

#ifndef FILEPROVENANCEELASTICDATAREADER_H
#define FILEPROVENANCEELASTICDATAREADER_H

#include "NdbDataReaders.h"
#include "FileProvenanceTableTailer.h"
#include "boost/date_time.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"
#include "rapidjson/document.h"
#include "tables/XAttrTable.h"
#include "tables/INodeTable.h"
#include "FileProvenanceConstants.h"

class FileProvenanceElasticDataReader : public NdbDataReader<FileProvenanceRow, SConn, PKeys> {
public:
  FileProvenanceElasticDataReader(SConn connection, const bool hopsworks, const int lru_cap);
  virtual ~FileProvenanceElasticDataReader();
private:
  XAttrTable mXAttrTable;
  XAttrTable mXAttrTrashBinTable;
  INodeTable mInodeTable;
  void processAddedandDeleted(Pq* data_batch, Bulk<PKeys>& bulk);
  boost::optional<XAttrRow> getXAttr(XAttrPK key);
  boost::tuple<string, string, string> processModelComp(FileProvenanceRow row);
  string mlModelId(XAttrRow mlIdXAttr);
  string process_row(FileProvenanceRow row);
  string bulk_add_json(FileProvenanceRow row, string mlType, string mlId, string mlDeps);
  string readable_timestamp(Int64 timestamp);
};

class FileProvenanceElasticDataReaders :  public NdbDataReaders<FileProvenanceRow, SConn, PKeys>{
  public:
    FileProvenanceElasticDataReaders(SConn* connections, int num_readers,const bool hopsworks,
          TimedRestBatcher<PKeys>* restEndpoint, const int lru_cap) : 
    NdbDataReaders(restEndpoint){
      for(int i=0; i<num_readers; i++){
        FileProvenanceElasticDataReader* dr 
        = new FileProvenanceElasticDataReader(connections[i], hopsworks, lru_cap);
        dr->start(i, this);
        mDataReaders.push_back(dr);
      }
    }
};

#endif /* FILEPROVENANCEELASTICDATAREADER_H */

