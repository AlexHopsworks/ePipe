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
 * File:   FileProvenanceJanusGraph.cpp
 * Author: Alexandru Ormenisan <aaor@kth.se>
 * 
 */

#include "FileProvenanceJanusGraph.h"

FileProvenanceJanusGraph::FileProvenanceJanusGraph(string janusgraph_addr,
        const bool stats, SConn conn) :
JanusGraphBase(janusgraph_addr),
mStats(stats), mConn(conn) {
  mJanusGraphBulkAddr = getJanusGraphUrl();
}

void FileProvenanceJanusGraph::process(vector<PBulk>* bulks) {
  PKeys keys;
  string batch;
  for (vector<PBulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
    PBulk bulk = *it;
    batch.append(bulk.mJSON);
    keys.insert(keys.end(), bulk.mPKs.begin(), bulk.mPKs.end());
  }

  //TODO: handle failures
  if (httpRequest(HTTP_POST, mJanusGraphBulkAddr, batch)) {
    if (!keys.empty()) {
      FileProvenanceLogTable().removeLogs(mConn, keys);
    }

  }
  //TODO: stats
}

FileProvenanceJanusGraph::~FileProvenanceJanusGraph() {

}

