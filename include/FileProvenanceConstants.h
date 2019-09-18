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

#ifndef FILEPROVENANCECONSTANTS_H
#define FILEPROVENANCECONSTANTS_H

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include "tables/AppProvenanceLogTable.h"
#include "tables/FileProvenanceLogTable.h"

namespace FileProvenanceConstants {
  const std::string XATTRS_ML_ID = "ml_id";
  const std::string XATTRS_FEATURES = "features";
  const std::string XATTRS_TRAINING_DATASETS = "training_datasets";
  const Int8 XATTRS_USER_NAMESPACE = 5;
  const std::string README_FILE = "README.md";
  
  const std::string ML_TYPE_NONE = "NONE";
  const std::string ML_TYPE_ERR = "ERR";
  const std::string ML_TYPE_MODEL = "MODEL";
  const std::string ML_TYPE_MODEL_PART = "MODEL_PART";
  const std::string ML_TYPE_FEATURE = "FEATURE";
  const std::string ML_TYPE_FEATURE_PART = "FEATURE_PART";
  const std::string ML_TYPE_TDATASET = "TRAINING_DATASET";
  const std::string ML_TYPE_TDATASET_PART = "TRAINING_DATASET_PART";
  const std::string ML_TYPE_EXPERIMENT = "EXPERIMENT";
  const std::string ML_TYPE_EXPERIMENT_PART = "EXPERIMENT_PART";

   enum MLType {
    NONE,
    MODEL,
    FEATURE,
    TRAINING_DATASET,
    EXPERIMENT,

    MODEL_PART,
    FEATURE_PART,
    TRAINING_DATASET_PART,
    EXPERIMENT_PART
  };

  inline static const std::string MLTypeToStr(MLType mlType) {
    switch (mlType) {
      case NONE:
        return ML_TYPE_NONE;
      case MODEL:
        return ML_TYPE_MODEL;
      case FEATURE:
        return ML_TYPE_FEATURE;
      case TRAINING_DATASET:
        return ML_TYPE_TDATASET;
      case EXPERIMENT:
        return ML_TYPE_EXPERIMENT;
      case MODEL_PART:
        return ML_TYPE_MODEL_PART;
      case FEATURE_PART:
        return ML_TYPE_FEATURE_PART;
      case TRAINING_DATASET_PART:
        return ML_TYPE_TDATASET_PART;
      case EXPERIMENT_PART:
        return ML_TYPE_EXPERIMENT_PART;
      default:
        return ML_TYPE_NONE;
    }
  };

  const std::string ML_ID_SPACE = "space_id";
  const std::string ML_ID_BASE = "id";
  const std::string ML_ID_VERSION = "version";

  const std::string H_OP_CREATE = "CREATE";
  const std::string H_OP_DELETE = "DELETE";
  const std::string H_OP_ACCESS_DATA = "ACCESS_DATA";
  const std::string H_OP_MODIFY_DATA = "MODIFY_DATA";
  const std::string H_OP_METADATA = "METADATA";
  const std::string H_OP_XATTR_ADD = "XATTR_ADD";
  const std::string H_OP_XATTR_UPDATE = "XATTR_UPDATE";
  const std::string H_OP_XATTR_DELETE = "XATTR_DELETE";
  const std::string H_OP_OTHER = "OTHER";

  const std::string H_XATTR_ML_ID = "ml_id";
  const std::string H_XATTR_ML_DEPS = "ml_deps";

  const std::string APP_SUBMITTED_STATE = "SUBMITTED";
  const std::string APP_RUNNING_STATE = "RUNNING";

  inline bool oneLvlDeep(FileProvenanceRow row) {
    return row.mDatasetId != row.mInodeId && row.mDatasetId == row.mParentId;
  }

  inline bool twoLvlDeep(FileProvenanceRow row) {
    return row.mDatasetId != row.mInodeId && row.mDatasetId != row.mParentId && row.mP1Name == "";
  }

  inline bool onePlusLvlDeep(FileProvenanceRow row) {
    return row.mDatasetId != row.mInodeId && row.mDatasetId != row.mParentId;
  }

  inline bool twoPlusLvlDeep(FileProvenanceRow row) {
    return row.mDatasetId != row.mInodeId && row.mDatasetId != row.mParentId && row.mP1Name != "";
  }

  inline std::string twoNameForAsset(FileProvenanceRow row) {
    std::stringstream  mlId;
    mlId << row.mParentName << "_" << row.mInodeName;
    return mlId.str();
  }

  inline std::string twoNameForPart(FileProvenanceRow row) {
    std::stringstream  mlId;
    if(row.mP2Name == "") {
      mlId << row.mP1Name << "_" << row.mParentName;
    } else {
      mlId << row.mP1Name << "_" << row.mP2Name;
    }
    return mlId.str();
  }

  inline std::string oneNameForPart(FileProvenanceRow row) {
    std::stringstream  mlId;
    if(row.mP1Name == "") {
      mlId << row.mParentName;
    } else {
      mlId << row.mP1Name;
    }
    return mlId.str();
  }

  inline bool isDatasetName1(FileProvenanceRow row, std::string part) {
    std::stringstream  mlDataset;
    mlDataset << part;
    return row.mDatasetName == mlDataset.str();
  }

  inline bool isDatasetName2(FileProvenanceRow row, std::string part) {
    std::stringstream  mlDataset;
    mlDataset << row.mProjectName << "_" << part;
    return row.mDatasetName == mlDataset.str();
  }

  inline bool isReadmeFile(FileProvenanceRow row) {
    return row.mInodeName == README_FILE;
  }

  inline bool isMLModel(FileProvenanceRow row) {
    return isDatasetName1(row, "Models") && twoLvlDeep(row);
  }

  inline bool partOfMLModel(FileProvenanceRow row) {
    return isDatasetName1(row, "Models") && twoPlusLvlDeep(row);
  }

  inline std::string getMLModelId(FileProvenanceRow row) {
    return twoNameForAsset(row);
  }

  inline std::string getMLModelParentId(FileProvenanceRow row) {
    return twoNameForPart(row);
  }

  inline bool isMLFeature(FileProvenanceRow row) {
    return isDatasetName2(row, "featurestore.db") && oneLvlDeep(row);
  }

  inline bool partOfMLFeature(FileProvenanceRow row) {
    return isDatasetName2(row, "featurestore.db") && onePlusLvlDeep(row);
  }

  inline std::string getMLFeatureId(FileProvenanceRow row) {
    return row.mInodeName;
  }

  inline std::string getMLFeatureParentId(FileProvenanceRow row) {
    return oneNameForPart(row);
  }

  inline bool isMLTDataset(FileProvenanceRow row) {
    return isDatasetName2(row, "Training_Datasets") && oneLvlDeep(row);
  }

  inline bool partOfMLTDataset(FileProvenanceRow row) {
    return isDatasetName2(row, "Training_Datasets") && onePlusLvlDeep(row);
  }

  inline std::string getMLTDatasetId(FileProvenanceRow row) {
    return row.mInodeName;
  }

  inline std::string getMLTDatasetParentId(FileProvenanceRow row) {
    return oneNameForPart(row);
  }

  inline bool isMLExperimentName(std::string name) {
    std::vector<std::string> strs;
    boost::split(strs,name,boost::is_any_of("_"));
    return boost::starts_with(name, "application_") && strs.size() == 4;
  }

  inline bool isMLExperiment(FileProvenanceRow row) {
    return isDatasetName1(row, "Experiments") && oneLvlDeep(row) 
      && isMLExperimentName(row.mInodeName);
  }

  inline bool partOfMLExperiment(FileProvenanceRow row) {
    return isDatasetName1(row, "Experiments") && onePlusLvlDeep(row) 
      && isMLExperimentName(row.mParentName);
  }

  inline std::string getMLExperimentId(FileProvenanceRow row) {
    return row.mInodeName;
  }

  inline std::string getMLExperimentParentId(FileProvenanceRow row) {
    return oneNameForPart(row);
  }


  inline std::pair <MLType, std::string> parseML(FileProvenanceRow row) {
    MLType mlType;
    std::string mlId;
    if(isReadmeFile(row)) {
      mlType = MLType::NONE;
      mlId = "";
    } else if(isMLModel(row)) {
      mlType = MLType::MODEL;
      mlId = getMLModelId(row);
    } else if(isMLTDataset(row)) {
      mlType = MLType::TRAINING_DATASET;
      mlId = getMLTDatasetId(row);
    } else if(isMLFeature(row)) {
      mlType = MLType::FEATURE;
      mlId = getMLFeatureId(row);
    } else if(isMLExperiment(row)) {
      mlType = MLType::EXPERIMENT;
      mlId = getMLExperimentId(row);
    } else if(partOfMLModel(row)) {
      mlType = MLType::MODEL_PART;
      mlId = getMLModelParentId(row);
    } else if(partOfMLTDataset(row)) {
      mlType = MLType::TRAINING_DATASET_PART;
      mlId = getMLTDatasetParentId(row);
    } else if(partOfMLFeature(row)) {
      mlType = MLType::FEATURE_PART;
      mlId = getMLFeatureParentId(row);
    } else if(partOfMLExperiment(row)) {
      mlType = MLType::EXPERIMENT_PART;
      mlId = getMLExperimentParentId(row);
    } else {
      mlType = MLType::NONE;
      mlId = "";
    }
    return std::make_pair(mlType, mlId);
  }
}

#endif /* FILEPROVENANCECONSTANTS_H */