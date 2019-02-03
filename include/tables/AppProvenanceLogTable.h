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
 * File:   AppProvenanceLogTable.h
 * Author: Alexandru Ormenisan <aaor@kth.se>
 *
 */

#ifndef APPPROVENANCELOGTABLE_H
#define APPPROVENANCELOGTABLE_H
#include "DBWatchTable.h"
#include "ConcurrentPriorityQueue.h"
#include "ConcurrentQueue.h"

struct AppProvenancePK {
  string mId;
  string mState;
  Int64 mTimestamp;

  AppProvenancePK(string id, string state, Int64 timestamp) {
    mId = id;
    mState = state;
    mTimestamp = timestamp;
  }

  string to_string() {
    stringstream out;
    out << mId << "-" << mState << "-" << mTimestamp;
    return out.str();
  }
};

struct AppProvenanceRow {
  string mId;
  string mState;
  Int64 mTimestamp;
  string mName;
  string mUser;

  ptime mEventCreationTime;

  AppProvenancePK getPK() {
    return AppProvenancePK(mId, mState, mTimestamp);
  }

  string to_string() {
    stringstream stream;
    stream << "-------------------------" << endl;
    stream << "Id = " << mId << endl;
    stream << "State = " << mState << endl;
    stream << "Timestamp = " << mTimestamp << endl;
    stream << "Name = " << mName << endl;
    stream << "User = " << mUser << endl;
    stream << "-------------------------" << endl;
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
typedef vector<AppProvenancePK> AppPKeys;
typedef vector<AppProvenanceRow> AppPq;

typedef vector<AppProvenanceRow> AppPv;
typedef boost::unordered_map<Uint64, AppPv* > AppProvenanceRowsByGCI;
typedef boost::tuple<vector<Uint64>*, AppProvenanceRowsByGCI* > AppProvenanceRowsGCITuple;

class AppProvenanceLogTable : public DBWatchTable<AppProvenanceRow> {
public:

  AppProvenanceLogTable() : DBWatchTable("yarn_app_provenance_log") {
    addColumn("id");
    addColumn("state");
    addColumn("timestamp");  
    addColumn("name");
    addColumn("user");
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
    return row;
  }

  void removeLogs(Ndb* connection, AppPKeys& pks) {
    start(connection);
    for (AppPKeys::iterator it = pks.begin(); it != pks.end(); ++it) {
      AppProvenancePK pk = *it;
      AnyMap a;
      a[0] = pk.mId;
      a[1] = pk.mState;
      a[2] = pk.mTimestamp;
      doDelete(a);
      LOG_DEBUG("Delete log row: " + pk.to_string());
    }
    end();
  }

};


#endif /* APPPROVENANCELOGTABLE_H */

