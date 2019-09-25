/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * ePipe is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * ePipe is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef NOTIFIER_H
#define NOTIFIER_H

#include "FsMutationsBatcher.h"
#include "SchemabasedMetadataBatcher.h"
#include "ProjectsElasticSearch.h"
#include "HopsworksOpsLogTailer.h"
#include "MetadataLogTailer.h"
#include "ClusterConnectionBase.h"
#include "hive/TBLSTailer.h"
#include "hive/SDSTailer.h"
#include "hive/PARTTailer.h"
#include "hive/IDXSTailer.h"
#include "hive/SkewedLocTailer.h"
#include "hive/SkewedValuesTailer.h"
#include "http/server/HttpServer.h"
#include "FileProvenanceElastic.h"
#include "FileProvenanceElasticDataReader.h"
#include "AppProvenanceElastic.h"
#include "AppProvenanceElasticDataReader.h"

class Notifier : public ClusterConnectionBase {
public:
  Notifier(const char* connection_string, const char* database_name,
          const char* meta_database_name, const char* hive_meta_database_name,
          const TableUnitConf mutations_tu, const TableUnitConf schemabased_tu,const TableUnitConf provenance_tu,
          const int poll_maxTimeToWait, const std::string elastic_addr, const bool hopsworks, const std::string elastic_index,
          const std::string elastic_file_provenance_index, const std::string elastic_app_provenance_index,
          const int elastic_batch_size, const int elastic_issue_time, const int lru_cap, const bool recovery, const bool stats,
          Barrier barrier, const bool hiveCleaner, const std::string
          metricsServer);
  void start();
  virtual ~Notifier();

private:

  const TableUnitConf mMutationsTU;
  const TableUnitConf mSchemabasedTU;
  const TableUnitConf mFileProvenanceTU;
  const TableUnitConf mAppProvenanceTU;

  const int mPollMaxTimeToWait;
  const std::string mElasticAddr;
  const bool mHopsworksEnabled;
  const std::string mElasticIndex;
  const std::string mElasticFileProvenanceIndex;
  const std::string mElasticAppProvenanceIndex;
  const int mElasticBatchsize;
  const int mElasticIssueTime;
  const int mLRUCap;
  const bool mRecovery;
  const bool mStats;
  const Barrier mBarrier;
  const bool mHiveCleaner;
  const std::string mMetricsServer;

  ProjectsElasticSearch* mProjectsElasticSearch;

  FsMutationsTableTailer* mFsMutationsTableTailer;
  FsMutationsDataReaders* mFsMutationsDataReaders;
  FsMutationsBatcher* mFsMutationsBatcher;

  MetadataLogTailer* mMetadataLogTailer;

  SchemabasedMetadataReaders* mSchemabasedMetadataReaders;
  SchemabasedMetadataBatcher* mSchemabasedMetadataBatcher;

  HopsworksOpsLogTailer* mhopsworksOpsLogTailer;

  FileProvenanceTableTailer* mFileProvenanceTableTailer;
  FileProvenanceElasticDataReaders* mFileProvenanceElasticDataReaders;
  RCBatcher<FileProvenanceRow, SConn>* mFileProvenanceBatcher;
  FileProvenanceElastic* mFileProvenanceElastic;

  AppProvenanceTableTailer* mAppProvenanceTableTailer;
  AppProvenanceElasticDataReaders* mAppProvenanceElasticDataReaders;
  RCBatcher<AppProvenanceRow, SConn>* mAppProvenanceBatcher;
  AppProvenanceElastic* mAppProvenanceElastic;

  TBLSTailer* mTblsTailer;
  SDSTailer* mSDSTailer;
  PARTTailer* mPARTTailer;
  IDXSTailer* mIDXSTailer;
  SkewedLocTailer* mSkewedLocTailer;
  SkewedValuesTailer* mSkewedValuesTailer;

  HttpServer* mHttpServer;
  MetricsProviders* mMetricsProviders;
  void setup();
};

#endif /* NOTIFIER_H */

