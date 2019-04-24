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
 * File:   AppProvenanceTableTailer.cpp
 * Author: Alexandru Ormenisan<aaor@kth.se>
 *
 */

#include "AppProvenanceTableTailer.h"

AppProvenanceTableTailer::AppProvenanceTableTailer(Ndb *ndb, const int poll_maxTimeToWait, const Barrier barrier)
: RCTableTailer(ndb, new AppProvenanceLogTable(), poll_maxTimeToWait, barrier) {
  mQueue = new AppCPRq();
  mCurrentPriorityQueue = new AppPRpq();
}

void AppProvenanceTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, AppProvenanceRow pre,
        AppProvenanceRow row) {
  mLock.lock();
  mCurrentPriorityQueue->push(row);
  int size = mCurrentPriorityQueue->size();
  mLock.unlock();

  LOG_TRACE("push provenance log for [" << row.mId << "] to queue[" << size << "]");

}

void AppProvenanceTableTailer::barrierChanged() {
  AppPRpq* pq = NULL;
  mLock.lock();
  if (!mCurrentPriorityQueue->empty()) {
    pq = mCurrentPriorityQueue;
    mCurrentPriorityQueue = new AppPRpq();
  }
  mLock.unlock();

  if (pq != NULL) {
    LOG_TRACE("--------------------------------------NEW BARRIER (" << pq->size() << " events )------------------- ");
    pushToQueue(pq);
  }
}

AppProvenanceRow AppProvenanceTableTailer::consume() {
  AppProvenanceRow row;
  mQueue->wait_and_pop(row);
  LOG_TRACE(" pop appid [" << row.mId << "] from queue \n" << row.to_string());
  return row;
}

void AppProvenanceTableTailer::pushToQueue(AppPRpq *curr) {
  while (!curr->empty()) {
    mQueue->push(curr->top());
    curr->pop();
  }
  delete curr;
}

void AppProvenanceTableTailer::pushToQueue(AppPv* curr) {
  std::sort(curr->begin(), curr->end(), AppProvenanceRowComparator());
  for (AppPv::reverse_iterator it = curr->rbegin(); it != curr->rend(); ++it) {
    mQueue->push(*it);
  }
  delete curr;
}

void AppProvenanceTableTailer::recover() {
  AppProvenanceRowsGCITuple tuple = AppProvenanceLogTable().getAllByGCI(mNdbConnection);
  vector<Uint64>* gcis = tuple.get<0>();
  AppProvenanceRowsByGCI* rowsByGCI = tuple.get<1>();
  for (vector<Uint64>::iterator it = gcis->begin(); it != gcis->end(); it++) {
    Uint64 gci = *it;
    pushToQueue(rowsByGCI->at(gci));
  }
}

AppProvenanceTableTailer::~AppProvenanceTableTailer() {
  delete mQueue;
}