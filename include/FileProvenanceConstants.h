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
 * File:   FileProvenanceConstants.h
 * Author: Alexandru Ormenisan <aaor@kth.se>
 * 
 */
#ifndef FILEPROVENANCECONSTANTS_H
#define FILEPROVENANCECONSTANTS_H

namespace FileProvenanceConstants {
  const string XATTRS_ML_ID = "ml_id";
  const string XATTRS_FEATURES = "features";
  const string XATTRS_TRAINING_DATASETS = "training_datasets";
  const Int8 XATTRS_USER_NAMESPACE = 0;
  
  const string ML_TYPE_NONE = "none";
  const string ML_TYPE_ERR = "err";
  const string ML_TYPE_MODEL = "model";
  const string ML_TYPE_FEATURE = "feature";
  const string ML_TYPE_TDATASET = "training_dataset";

  const string ML_ID_SPACE = "space_id";
  const string ML_ID_BASE = "id";
  const string ML_ID_VERSION = "version";

  const string H_OP_CREATE = "CREATE";
  const string H_OP_DELETE = "DELETE";
  const string H_OP_ACCESS_DATA = "ACCESS_DATA";
  const string H_OP_MODIFY_DATA = "MODIFY_DATA";
  const string H_OP_METADATA = "METADATA";
  const string H_OP_XATTR_ADD = "XATTR_ADD";
  const string H_OP_OTHER = "OTHER";

  const string H_XATTR_ML_ID = "ml_id";
  const string H_XATTR_ML_DEPS = "ml_deps";

  inline bool isMLModel(FileProvenanceRow row) {
    stringstream mlDataset;
    mlDataset << "Models";
    return row.mDatasetId == row.mP2Id && row.mDatasetName == mlDataset.str();
  }

  inline bool isMLFeature(FileProvenanceRow row) {
    stringstream mlDataset;
    mlDataset << row.mProjectName << "_featurestore.db" ;
    return row.mDatasetId == row.mP1Id && row.mDatasetName == mlDataset.str();
  }

  inline bool isMLTDataset(FileProvenanceRow row) {
    stringstream mlDataset;
    mlDataset << row.mProjectName << "_Training_Datasets" ;
    return row.mDatasetId == row.mP1Id && row.mDatasetName == mlDataset.str();
  }
}

#endif /* FILEPROVENANCECONSTANTS_H */