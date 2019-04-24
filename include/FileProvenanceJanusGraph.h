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
 * File:   FileProvenanceJanusGraph.h
 * Author: Alexandru Ormenisan <aaor@kth.se>
 *
 */

#ifndef FILEPROVENANCEJANUSGRAPH_H
#define FILEPROVENANCEJANUSGRAPH_H

#include "JanusGraphBase.h"
#include "FileProvenanceTableTailer.h"

typedef Bulk<PKeys> PBulk;

class FileProvenanceJanusGraph : public JanusGraphBase<PKeys> {
public:
  FileProvenanceJanusGraph(string janusgraph_addr,
          const bool stats,
          SConn conn);
  virtual ~FileProvenanceJanusGraph();
private:
  const bool mStats;

  string mJanusGraphBulkAddr;

  SConn mConn;

  virtual void process(vector<PBulk>* bulks);

};

#endif /* FILEPROVENANCEJANUSGRAPH_H */

