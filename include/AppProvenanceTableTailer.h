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

#ifndef APPPROVENANCETABLETAILER_H
#define APPPROVENANCETABLETAILER_H

#include "RCTableTailer.h"
#include "tables/AppProvenanceLogTable.h"

class AppProvenanceTableTailer : public RCTableTailer<AppProvenanceRow> {
public:
  AppProvenanceTableTailer(Ndb* ndb, Ndb* ndbRecovery, const int poll_maxTimeToWait, const Barrier barrier);
  AppProvenanceRow consume();
  virtual ~AppProvenanceTableTailer();

private:
  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, AppProvenanceRow pre, AppProvenanceRow row);
  void barrierChanged();

  void pushToQueue(AppPRpq* curr);

  AppCPRq *mQueue;
  AppPRpq* mCurrentPriorityQueue;
  boost::mutex mLock;

};


#endif //APPPROVENANCETABLETAILER_H
