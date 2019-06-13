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
  const string ML_TYPE_MODEL_PART = "model_part";
  const string ML_TYPE_FEATURE = "feature";
  const string ML_TYPE_FEATURE_PART = "feature_part";
  const string ML_TYPE_TDATASET = "training_dataset";
  const string ML_TYPE_TDATASET_PART = "training_dataset_part";
  const string ML_TYPE_EXPERIMENT = "experiment";
  const string ML_TYPE_EXPERIMENT_PART = "experiment_part";

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

  inline bool oneLvlDeep(FileProvenanceRow row) {
    return row.mDatasetId != row.mInodeId && row.mDatasetId == row.mParentId;
  }

  inline bool twoLvlDeep(FileProvenanceRow row) {
    return row.mDatasetId != row.mInodeId && row.mDatasetId != row.mParentId && row.mP1Name == "";
  }

  inline bool twoPlusLvlDeep(FileProvenanceRow row) {
    return row.mDatasetId != row.mInodeId && row.mDatasetId != row.mParentId && row.mP1Name != "";
  }

  inline string twoNameForAsset(FileProvenanceRow row) {
    stringstream mlId;
    mlId << row.mParentName << "_" << row.mInodeName;
    return mlId.str();
  }

  inline string twoNameForPart(FileProvenanceRow row) {
    stringstream mlId;
    if(row.mP2Name == "") {
      mlId << row.mP1Name << "_" << row.mParentName;
    } else {
      mlId << row.mP1Name << "_" << row.mP2Name;
    }
    return mlId.str();
  }

  inline string oneNameForPart(FileProvenanceRow row) {
    stringstream mlId;
    if(row.mP1Name == "") {
      mlId << row.mParentName;
    } else {
      mlId << row.mP1Name;
    }
    return mlId.str();
  }

  inline bool isDatasetName1(FileProvenanceRow row, string part) {
    stringstream mlDataset;
    mlDataset << part;
    return row.mDatasetName == mlDataset.str();
  }

  inline bool isDatasetName2(FileProvenanceRow row, string part) {
    stringstream mlDataset;
    mlDataset << row.mProjectName << "_" << part;
    return row.mDatasetName == mlDataset.str();
  }

  inline bool isMLModel(FileProvenanceRow row) {
    return isDatasetName1(row, "Models") && twoLvlDeep(row);
  }

  inline bool partOfMLModel(FileProvenanceRow row) {
    return isDatasetName1(row, "Models") && twoPlusLvlDeep(row);
  }

  inline string getMLModelId(FileProvenanceRow row) {
    return twoNameForAsset(row);
  }

  inline string getMLModelParentId(FileProvenanceRow row) {
    return twoNameForPart(row);
  }

  inline bool isMLFeature(FileProvenanceRow row) {
    return isDatasetName2(row, "_featurestore.db") && oneLvlDeep(row);
  }

  inline bool partOfMLFeature(FileProvenanceRow row) {
    return isDatasetName2(row, "_featurestore.db") && twoLvlDeep(row);
  }

  inline string getMLFeatureId(FileProvenanceRow row) {
    return row.mInodeName;
  }

  inline string getMLFeatureParentId(FileProvenanceRow row) {
    return oneNameForPart(row);
  }

  inline bool isMLTDataset(FileProvenanceRow row) {
    return isDatasetName2(row, "_Training_Datasets") && oneLvlDeep(row);
  }

  inline bool partOfMLTDataset(FileProvenanceRow row) {
    return isDatasetName2(row, "_Training_Datasets") && twoLvlDeep(row);
  }

  inline string getMLTDatasetId(FileProvenanceRow row) {
    return row.mInodeName;
  }

  inline string getMLTDatasetParentId(FileProvenanceRow row) {
    return oneNameForPart(row);
  }

  inline bool isMLExperiment(FileProvenanceRow row) {
    return isDatasetName1(row, "Experiments") && oneLvlDeep(row);
  }

  inline bool partOfMLExperiment(FileProvenanceRow row) {
    return isDatasetName1(row, "Experiments") && twoLvlDeep(row);
  }

  inline string getMLExperimentId(FileProvenanceRow row) {
    return row.mInodeName;
  }

  inline string getMLExperimentParentId(FileProvenanceRow row) {
    return oneNameForPart(row);
  }
}

#endif /* FILEPROVENANCECONSTANTS_H */