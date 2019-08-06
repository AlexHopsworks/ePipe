/*
 * Copyright (C) 2018 Logical Clocks AB
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
 * File:   ProvenanceBatcher.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef PROVENANCEBATCHER_H
#define PROVENANCEBATCHER_H
#include "RCBatcher.h"
#include "ProvenanceTableTailer.h"
#include "ProvenanceDataReader.h"

class ProvenanceBatcher : public RCBatcher<ProvenanceRow, SConn, PKeys> {
public:

  ProvenanceBatcher(ProvenanceTableTailer* table_tailer, ProvenanceDataReaders* data_reader,
          const int time_before_issuing_ndb_reqs, const int batch_size)
  : RCBatcher(table_tailer, data_reader, time_before_issuing_ndb_reqs, batch_size) {

  }

};

#endif /* PROVENANCEBATCHER_H */
