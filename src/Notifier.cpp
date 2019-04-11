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
 * File:   Notifier.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "Notifier.h"

Notifier::Notifier(const char* connection_string, const char* database_name, const char* meta_database_name,
        const TableUnitConf mutations_tu, const TableUnitConf schemabased_tu, const TableUnitConf schemaless_tu, TableUnitConf provenance_tu,
        const int poll_maxTimeToWait, const string elastic_ip, const bool hopsworks, const string elastic_index, const string elastic_provenance_index,
        const int elastic_batch_size, const int elastic_issue_time, const int lru_cap, const bool recovery,
        const bool stats, const string janusgraph_ip, Barrier barrier)
: ClusterConnectionBase(connection_string, database_name, meta_database_name), mMutationsTU(mutations_tu), mSchemabasedTU(schemabased_tu),
mSchemalessTU(schemaless_tu), mProvenanceTU(provenance_tu), mPollMaxTimeToWait(poll_maxTimeToWait), mElasticAddr(elastic_ip), mHopsworksEnabled(hopsworks),
mElasticIndex(elastic_index), mElasticProvenanceIndex(elastic_provenance_index),
mElasticBatchsize(elastic_batch_size), mElasticIssueTime(elastic_issue_time), mLRUCap(lru_cap),
mRecovery(recovery), mStats(stats), mJanusGraphAddr(janusgraph_ip), mBarrier(barrier) {
  setup();
}

void Notifier::start() {
  LOG_INFO("ePipe starting...");
  ptime t1 = getCurrentTime();

  if (mMutationsTU.isEnabled()) {
    mFsMutationsDataReaders->start();
    mFsMutationsBatcher->start();
    mFsMutationsTableTailer->start(mRecovery);
  }

  if (mSchemabasedTU.isEnabled()) {
    mSchemabasedMetadataReaders->start();
    mSchemabasedMetadataBatcher->start();
    mMetadataLogTailer->start(mRecovery);
  }

  if (mSchemalessTU.isEnabled()) {
    mSchemalessMetadataReaders->start();
    mSchemalessMetadataBatcher->start();
    mMetadataLogTailer->start(mRecovery);
  }

  if (mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled()
          || mSchemalessTU.isEnabled() || mHopsworksEnabled) {
    mProjectsElasticSearch->start();
  }

  if (mHopsworksEnabled) {
    mhopsworksOpsLogTailer->start(mRecovery);
  }

  if (mProvenanceTU.isEnabled()) {
    // mFileProvenancElasticSearch->start();

    mFileProvenanceJanusGraph->start();
    mFileProvenanceDataReaders->start();
    mFileProvenanceBatcher->start();
    mFileProvenanceTableTailer->start(mRecovery);

    mAppProvenanceJanusGraph->start();
    mAppProvenanceDataReaders->start();
    mAppProvenanceBatcher->start();
    mAppProvenanceTableTailer->start(mRecovery);
  }

  ptime t2 = getCurrentTime();
  LOG_INFO("ePipe started in " << getTimeDiffInMilliseconds(t1, t2) << " msec");

  if (mMutationsTU.isEnabled()) {
    mFsMutationsBatcher->waitToFinish();
    mFsMutationsTableTailer->waitToFinish();
  }

  if (mSchemabasedTU.isEnabled()) {
    mSchemabasedMetadataBatcher->waitToFinish();
    mMetadataLogTailer->waitToFinish();
  }

  if (mSchemalessTU.isEnabled()) {
    mSchemalessMetadataBatcher->waitToFinish();
    mMetadataLogTailer->waitToFinish();
  }

  if (mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled()
          || mSchemalessTU.isEnabled() || mHopsworksEnabled) {
    mProjectsElasticSearch->waitToFinish();
  }

  if (mHopsworksEnabled) {
    mhopsworksOpsLogTailer->waitToFinish();
  }

  if (mProvenanceTU.isEnabled()) {
    mFileProvenancElasticSearch->waitToFinish();

    mFileProvenanceBatcher->waitToFinish();
    mFileProvenanceTableTailer->waitToFinish();
    mFileProvenanceJanusGraph->waitToFinish();

    mAppProvenanceBatcher->waitToFinish();
    mAppProvenanceTableTailer->waitToFinish();
    mAppProvenanceJanusGraph->waitToFinish();
  }
}

void Notifier::setup() {
  if (mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled()
          || mSchemalessTU.isEnabled() || mHopsworksEnabled) {
    MConn ndb_connections_elastic;
    ndb_connections_elastic.metadataConnection = create_ndb_connection(mMetaDatabaseName);
    ndb_connections_elastic.inodeConnection = create_ndb_connection(mDatabaseName);

    mProjectsElasticSearch = new ProjectsElasticSearch(mElasticAddr, mElasticIndex,
            mElasticIssueTime, mElasticBatchsize, mStats, ndb_connections_elastic);
  }


  if (mMutationsTU.isEnabled()) {
    Ndb* mutations_tailer_connection = create_ndb_connection(mDatabaseName);
    mFsMutationsTableTailer = new FsMutationsTableTailer(mutations_tailer_connection, mPollMaxTimeToWait, mBarrier);

    MConn* mutations_connections = new MConn[mMutationsTU.mNumReaders];
    for (int i = 0; i < mMutationsTU.mNumReaders; i++) {
      mutations_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
      mutations_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
    }

    mFsMutationsDataReaders = new FsMutationsDataReaders(mutations_connections, mMutationsTU.mNumReaders,
            mHopsworksEnabled, mProjectsElasticSearch, mLRUCap);
    mFsMutationsBatcher = new FsMutationsBatcher(mFsMutationsTableTailer, mFsMutationsDataReaders,
            mMutationsTU.mWaitTime, mMutationsTU.mBatchSize);
  }


  if (mSchemabasedTU.isEnabled() || mSchemalessTU.isEnabled()) {
    Ndb* metadata_tailer_connection = create_ndb_connection(mMetaDatabaseName);
    mMetadataLogTailer = new MetadataLogTailer(metadata_tailer_connection, mPollMaxTimeToWait, mBarrier);
  }

  if (mSchemabasedTU.isEnabled()) {

    MConn* metadata_connections = new MConn[mSchemabasedTU.mNumReaders];
    for (int i = 0; i < mSchemabasedTU.mNumReaders; i++) {
      metadata_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
      metadata_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
    }

    mSchemabasedMetadataReaders = new SchemabasedMetadataReaders(metadata_connections, mSchemabasedTU.mNumReaders,
            mHopsworksEnabled, mProjectsElasticSearch, mLRUCap);
    mSchemabasedMetadataBatcher = new SchemabasedMetadataBatcher(mMetadataLogTailer, mSchemabasedMetadataReaders,
            mSchemabasedTU.mWaitTime, mSchemabasedTU.mBatchSize);
  }

  if (mSchemalessTU.isEnabled()) {

    MConn* s_metadata_connections = new MConn[mSchemalessTU.mNumReaders];
    for (int i = 0; i < mSchemalessTU.mNumReaders; i++) {
      s_metadata_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
      s_metadata_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
    }

    mSchemalessMetadataReaders = new SchemalessMetadataReaders(s_metadata_connections, mSchemalessTU.mNumReaders,
            mHopsworksEnabled, mProjectsElasticSearch);
    mSchemalessMetadataBatcher = new SchemalessMetadataBatcher(mMetadataLogTailer,
            mSchemalessMetadataReaders, mSchemalessTU.mWaitTime, mSchemalessTU.mBatchSize);
  }

  if (mHopsworksEnabled) {
    Ndb* ops_log_tailer_connection = create_ndb_connection(mMetaDatabaseName);
    mhopsworksOpsLogTailer = new HopsworksOpsLogTailer(ops_log_tailer_connection, mPollMaxTimeToWait, mBarrier,
            mProjectsElasticSearch, mLRUCap);
  }

  if (mProvenanceTU.isEnabled()) {
    // mFileProvenancElasticSearch = new ProvenanceElasticSearch(mElasticAddr,
    //         mElasticProvenanceIndex, mElasticIssueTime,
    //         mElasticBatchsize, mStats, ndb_file_provenance_conn);
    // for (int i = 0; i < mProvenanceTU.mNumReaders; i++) {
    //   provenance_connections[i] = create_ndb_connection(mDatabaseName);
    // }
    // mProvenanceDataReaders = new ProvenanceDataReaders(provenance_connections, mProvenanceTU.mNumReaders,
    //         mHopsworksEnabled, mProvenancElasticSearch);

    Ndb* ndb_file_provenance_conn = create_ndb_connection(mDatabaseName);
    mFileProvenanceJanusGraph = new FileProvenanceJanusGraph(mJanusGraphAddr, mElasticIssueTime,
            mElasticBatchsize, mStats, ndb_file_provenance_conn);
    Ndb* file_provenance_tailer_connection = create_ndb_connection(mDatabaseName);
    mFileProvenanceTableTailer = new FileProvenanceTableTailer(file_provenance_tailer_connection, mPollMaxTimeToWait, mBarrier);
    SConn* file_provenance_connections = new SConn[mProvenanceTU.mNumReaders];
    for (int i = 0; i < mProvenanceTU.mNumReaders; i++) {
      file_provenance_connections[i] = create_ndb_connection(mDatabaseName);
    }
    mFileProvenanceDataReaders = new FileProvenanceGremlinDataReaders(file_provenance_connections, mProvenanceTU.mNumReaders, 
            mHopsworksEnabled, mFileProvenanceJanusGraph, mLRUCap);
    mFileProvenanceBatcher = new RCBatcher<FileProvenanceRow, SConn, PKeys>(mFileProvenanceTableTailer, mFileProvenanceDataReaders,
            mProvenanceTU.mWaitTime, mProvenanceTU.mBatchSize);

    Ndb* ndb_app_provenance_conn = create_ndb_connection(mDatabaseName);
    mAppProvenanceJanusGraph = new AppProvenanceJanusGraph(mJanusGraphAddr, mElasticIssueTime,
            mElasticBatchsize, mStats, ndb_app_provenance_conn);
    Ndb* app_provenance_tailer_connection = create_ndb_connection(mDatabaseName);  
    mAppProvenanceTableTailer = new AppProvenanceTableTailer(app_provenance_tailer_connection, mPollMaxTimeToWait, mBarrier);
    SConn* app_provenance_connections = new SConn[mProvenanceTU.mNumReaders];
    for (int i = 0; i < mProvenanceTU.mNumReaders; i++) {
      app_provenance_connections[i] = create_ndb_connection(mDatabaseName);
    }
    mAppProvenanceDataReaders = new AppProvenanceGremlinDataReaders(app_provenance_connections, mProvenanceTU.mNumReaders, 
            mHopsworksEnabled, mAppProvenanceJanusGraph);
    mAppProvenanceBatcher = new RCBatcher<AppProvenanceRow, SConn, AppPKeys>(mAppProvenanceTableTailer, mAppProvenanceDataReaders,
            mProvenanceTU.mWaitTime, mProvenanceTU.mBatchSize);
  }
}

Notifier::~Notifier() {
  delete mFsMutationsTableTailer;
  delete mFsMutationsDataReaders;
  delete mFsMutationsBatcher;
  delete mMetadataLogTailer;
  delete mSchemabasedMetadataReaders;
  delete mSchemabasedMetadataBatcher;
  delete mSchemalessMetadataReaders;
  delete mSchemalessMetadataBatcher;
  ndb_end(2);
}
