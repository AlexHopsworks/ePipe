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
 * File:   AppProvenanceJanusGraph.cpp
 * Author: Alexandru Ormenisan <aaor@kth.se>
 * 
 */

#include "AppProvenanceJanusGraph.h"

AppProvenanceJanusGraph::AppProvenanceJanusGraph(string janusgraph_addr,
        int time_to_wait_before_inserting,
        int bulk_size, const bool stats, SConn conn) :
JanusGraphBase(janusgraph_addr, time_to_wait_before_inserting, bulk_size),
mStats(stats), mConn(conn) {
  mJanusGraphBulkAddr = getJanusGraphUrl();
}

void AppProvenanceJanusGraph::process(vector<AppPBulk>* bulks) {
  AppPKeys keys;
  string batch;
  for (vector<AppPBulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
    AppPBulk bulk = *it;
    batch.append(bulk.mJSON);
    keys.insert(keys.end(), bulk.mPKs.begin(), bulk.mPKs.end());
  }

  //TODO: handle failures
  if (httpRequest(HTTP_POST, mJanusGraphBulkAddr, batch)) {
    if (!keys.empty()) {
      AppProvenanceLogTable().removeLogs(mConn, keys);
    }

  }
  //TODO: stats
}

AppProvenanceJanusGraph::~AppProvenanceJanusGraph() {

}

