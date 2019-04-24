/*
 * Copyright (C) 2016 Hops.io
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
 * File:   Notifier.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef NOTIFIER_H
#define NOTIFIER_H

#include "FsMutationsBatcher.h"
#include "SchemabasedMetadataBatcher.h"
#include "ProjectsElasticSearch.h"
#include "ProvenanceElasticSearch.h"
#include "SchemalessMetadataReader.h"
#include "SchemalessMetadataBatcher.h"
#include "HopsworksOpsLogTailer.h"
#include "MetadataLogTailer.h"
#include "ClusterConnectionBase.h"
#include "FileProvenanceJanusGraph.h"
#include "JanusGraphBase.h"
#include "FileProvenanceGremlinDataReader.h"
#include "AppProvenanceGremlinDataReader.h"

class Notifier : public ClusterConnectionBase {
public:
  Notifier(const char* connection_string, const char* database_name, const char* meta_database_name,
          const TableUnitConf mutations_tu, const TableUnitConf schemabased_tu, const TableUnitConf schemaless_tu, const TableUnitConf provenance_tu,
          const int poll_maxTimeToWait, const string elastic_addr, const bool hopsworks, const string elastic_index, const string elastic_provenance_index,
          const int elastic_batch_size, const int elastic_issue_time, const int lru_cap, const bool recovery, const bool stats, const string janusgraph_ip,
          Barrier barrier);
  void start();
  virtual ~Notifier();

private:

  const TableUnitConf mMutationsTU;
  const TableUnitConf mSchemabasedTU;
  const TableUnitConf mSchemalessTU;
  const TableUnitConf mProvenanceTU;

  const int mPollMaxTimeToWait;
  const string mElasticAddr;
  const bool mHopsworksEnabled;
  const string mElasticIndex;
  const string mElasticProvenanceIndex;
  const int mElasticBatchsize;
  const int mElasticIssueTime;
  const int mLRUCap;
  const bool mRecovery;
  const bool mStats;
  const string mJanusGraphAddr;
  const Barrier mBarrier;

  ProjectsElasticSearch* mProjectsElasticSearch;

  FsMutationsTableTailer* mFsMutationsTableTailer;
  FsMutationsDataReaders* mFsMutationsDataReaders;
  FsMutationsBatcher* mFsMutationsBatcher;

  MetadataLogTailer* mMetadataLogTailer;

  SchemabasedMetadataReaders* mSchemabasedMetadataReaders;
  SchemabasedMetadataBatcher* mSchemabasedMetadataBatcher;

  SchemalessMetadataReaders* mSchemalessMetadataReaders;
  SchemalessMetadataBatcher* mSchemalessMetadataBatcher;

  HopsworksOpsLogTailer* mhopsworksOpsLogTailer;

  ProvenanceElasticSearch* mFileProvenancElasticSearch;
  
  FileProvenanceTableTailer* mFileProvenanceTableTailer;
  NdbDataReaders<FileProvenanceRow, SConn, PKeys>* mFileProvenanceDataReaders;
  RCBatcher<FileProvenanceRow, SConn, PKeys>* mFileProvenanceBatcher;
  FileProvenanceJanusGraph* mFileProvenanceJanusGraph;


  AppProvenanceTableTailer* mAppProvenanceTableTailer;
  NdbDataReaders<AppProvenanceRow, SConn, AppPKeys>* mAppProvenanceDataReaders;
  RCBatcher<AppProvenanceRow, SConn, AppPKeys>* mAppProvenanceBatcher;
  AppProvenanceJanusGraph* mAppProvenanceJanusGraph;

  void setup();
};

#endif /* NOTIFIER_H */

