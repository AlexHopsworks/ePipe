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

#ifndef APPPROVENANCELOGTABLE_H
#define APPPROVENANCELOGTABLE_H
#include "DBWatchTable.h"
#include "ConcurrentPriorityQueue.h"
#include "ConcurrentQueue.h"

struct AppProvenancePK {
  std::string mId;
  std::string mState;
  Int64 mTimestamp;

  AppProvenancePK(std::string id, std::string state, Int64 timestamp) {
    mId = id;
    mState = state;
    mTimestamp = timestamp;
  }

  std::string to_string() {
    std::stringstream  out;
    out << mId << "-" << mState << "-" << mTimestamp;
    return out.str();
  }
};

struct AppProvenanceRow {
  std::string mId;
  std::string mState;
  Int64 mTimestamp;
  std::string mName;
  std::string mUser;
  Int64 mSubmitTime;
  Int64 mStartTime;
  Int64 mFinishTime;

  ptime mEventCreationTime;

  AppProvenancePK getPK() {
    return AppProvenancePK(mId, mState, mTimestamp);
  }

  std::string to_string() {
    std::stringstream  stream;
    stream << "-------------------------" << std::endl;
    stream << "Id = " << mId << std::endl;
    stream << "State = " << mState << std::endl;
    stream << "Timestamp = " << mTimestamp << std::endl;
    stream << "Name = " << mName << std::endl;
    stream << "User = " << mUser << std::endl;
    stream << "SubmitTime = " << mSubmitTime << std::endl;
    stream << "StartTime = " << mStartTime << std::endl;
    stream << "FinishTime = " << mFinishTime << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }
};

struct AppProvenanceRowEqual {

  bool operator()(const AppProvenanceRow &lhs, const AppProvenanceRow &rhs) const {
    return lhs.mId == rhs.mId && lhs.mState == rhs.mState && lhs.mTimestamp == rhs.mTimestamp;
  }
};

struct AppProvenanceRowHash {

  std::size_t operator()(const AppProvenanceRow &a) const {
    std::size_t seed = 0;
    boost::hash_combine(seed, a.mId);
    boost::hash_combine(seed, a.mState);
    boost::hash_combine(seed, a.mTimestamp);
    return seed;
  }
};

struct AppProvenanceRowComparator {

  bool operator()(const AppProvenanceRow &r1, const AppProvenanceRow &r2) const {
    int res = r1.mId.compare(r2.mId);
    if (res == 0) {
      res = r1.mState.compare(r2.mState);
      if(res == 0) {
        return r1.mTimestamp > r2.mTimestamp;
      } else {
        return res > 0;
      }
    } else {
      return res > 0;
    }
  }
};

typedef ConcurrentQueue<AppProvenanceRow> AppCPRq;
typedef boost::heap::priority_queue<AppProvenanceRow, boost::heap::compare<AppProvenanceRowComparator> > AppPRpq;
typedef std::vector <boost::optional<AppProvenancePK> > AppPKeys;
typedef std::vector <AppProvenanceRow> AppPq;

typedef std::vector <AppProvenanceRow> AppPv;
typedef boost::unordered_map<Uint64, AppPv* > AppProvenanceRowsByGCI;
typedef boost::tuple<std::vector<Uint64>*, AppProvenanceRowsByGCI* > AppProvenanceRowsGCITuple;

class AppProvenanceLogTable : public DBWatchTable<AppProvenanceRow> {
public:

  AppProvenanceLogTable() : DBWatchTable("yarn_app_provenance_log") {
    addColumn("id");
    addColumn("state");
    addColumn("timestamp");  
    addColumn("name");
    addColumn("user");
    addColumn("submit_time");
    addColumn("start_time");
    addColumn("finish_time");
    addWatchEvent(NdbDictionary::Event::TE_INSERT);
  }

  AppProvenanceRow getRow(NdbRecAttr* value[]) {
    AppProvenanceRow row;
    row.mEventCreationTime = Utils::getCurrentTime();
    row.mId = get_string(value[0]);
    row.mState = get_string(value[1]);
    row.mTimestamp = value[2]->int64_value();
    row.mName = get_string(value[3]);
    row.mUser = get_string(value[4]);
    row.mSubmitTime = value[5]->int64_value();
    row.mStartTime = value[6]->int64_value();
    row.mFinishTime = value[7]->int64_value();

    return row;
  }

  void removeLogs(Ndb* connection, AppPKeys& pks) {
    start(connection);
    for (AppPKeys::iterator it = pks.begin(); it != pks.end(); ++it) {
      boost::optional<AppProvenancePK> pk = *it;
      if(pk) {
        AnyMap a;
        a[0] = pk.get().mId;
        a[1] = pk.get().mState;
        a[2] = pk.get().mTimestamp;
        doDelete(a);
        LOG_DEBUG("Delete log row: " + pk.get().to_string());
      }
    }
    end();
  }

  std::string getPKStr(AppProvenanceRow row) {
    return row.getPK().to_string();
  }
};


#endif /* APPPROVENANCELOGTABLE_H */

