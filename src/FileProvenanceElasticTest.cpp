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

#include "FileProvenanceElasticTest.h"

FileProvenanceElasticTest::FileProvenanceElasticTest() {}

//replay one by one the events between the cursor positions
void FileProvenanceElasticTest::processPerEvent(bulkCursor* cursor, std::vector<testBulk>* bulks) {
  std::vector<testBulk>::iterator itB, endB;
  std::vector<testEvent>::iterator itE, endE;
  if(cursor->bulkStartOffset == cursor->bulkEndOffset) {
    LOG_INFO("one bulk");
    itB = bulks->begin();
    std::advance(itB, cursor->bulkStartOffset);
    LOG_INFO("itB");
    testBulk bulk = *itB;
    LOG_INFO("size:" << bulk.mEvents.size());
    itE = bulk.mEvents.begin();
    std::advance(itE, cursor->inBulkStartOffset);
    LOG_INFO("itE");
    endE = bulk.mEvents.begin();
    std::advance(endE, cursor->inBulkEndOffset);
    LOG_INFO("endE");
    for(;std::distance(itE, endE) >= 0; ++itE) {
      testEvent event = *itE;
      LOG_INFO("val:" << event.val);
    }
  } else {
    LOG_INFO("multi bulk");
    itB = bulks->begin();
    std::advance(itB, cursor->bulkStartOffset);
    LOG_INFO("itB");
    endB = bulks->begin();
    std::advance(endB, cursor->bulkEndOffset);
    LOG_INFO("endB");

    LOG_INFO("first bulk");
    testBulk firstBulk = *itB;
    LOG_INFO("size:" << firstBulk.mEvents.size());
    itE = firstBulk.mEvents.begin();
    std::advance(itE, cursor->inBulkStartOffset);
    LOG_INFO("itE");
    for(;itE != firstBulk.mEvents.end(); ++itE) {
      testEvent event = *itE;
      LOG_INFO("val:" << event.val);
    }
    ++itB;

    LOG_INFO("mid bulks");
    for(;std::distance(itB, endB) > 0; ++itB) {
      testBulk midBulk = *itB;
      for (itE = midBulk.mEvents.begin(); itE != midBulk.mEvents.end(); ++itE) {
        testEvent event = *itE;
        LOG_INFO("val:" << event.val);
      }
    }

    LOG_INFO("last bulk");
    testBulk lastBulk = *itB;
    LOG_INFO("size:" << lastBulk.mEvents.size());
    endE = lastBulk.mEvents.begin();
    std::advance(itE, cursor->inBulkEndOffset);
    for(itE = lastBulk.mEvents.begin();std::distance(itE, endE) >= 0; ++itE) {
      testEvent event = *itE;
      LOG_INFO("val:" << event.val);
    }
  }
}

void FileProvenanceElasticTest::test() {
  std::vector<testBulk>* v = new std::vector<testBulk>();
  testEvent e1_1, e1_2, e1_3;
  e1_1.val = "1_1";
  e1_2.val = "1_2";
  e1_3.val = "1_3";
  testBulk b1;
  b1.mEvents.push_back(e1_1);
  b1.mEvents.push_back(e1_2);
  b1.mEvents.push_back(e1_3);
  LOG_INFO("b1 size:" << b1.mEvents.size());
  v->push_back(b1);

  testEvent e2_1, e2_2, e2_3;
  e2_1.val = "2_1";
  e2_2.val = "2_2";
  e2_3.val = "2_3";
  testBulk b2;
  b2.mEvents.push_back(e2_1);
  b2.mEvents.push_back(e2_2);
  b2.mEvents.push_back(e2_3);
  LOG_INFO("b2 size:" << b2.mEvents.size());
  v->push_back(b2);

  testEvent e3_1, e3_2, e3_3;
  e3_1.val = "3_1";
  e3_2.val = "3_2";
  e3_3.val = "3_3";
  testBulk b3;
  b3.mEvents.push_back(e3_1);
  b3.mEvents.push_back(e3_2);
  b3.mEvents.push_back(e3_3);
  LOG_INFO("b3 size:" << b3.mEvents.size());
  v->push_back(b3);
  LOG_INFO("v size:" << v->size());


  bulkCursor* cursor1 = new bulkCursor;
  cursor1->bulkStartOffset = 0;
  cursor1->bulkEndOffset = 0;
  cursor1->inBulkStartOffset = 0;
  cursor1->inBulkEndOffset=0;

  bulkCursor* cursor2 = new bulkCursor;
  cursor2->bulkStartOffset = 0;
  cursor2->bulkEndOffset = 0;
  cursor2->inBulkStartOffset = 2;
  cursor2->inBulkEndOffset=2;

  bulkCursor* cursor3 = new bulkCursor;
  cursor3->bulkStartOffset = 0;
  cursor3->bulkEndOffset = 0;
  cursor3->inBulkStartOffset = 1;
  cursor3->inBulkEndOffset=1;

  bulkCursor* cursor4 = new bulkCursor;
  cursor4->bulkStartOffset = 0;
  cursor4->bulkEndOffset = 0;
  cursor4->inBulkStartOffset = 0;
  cursor4->inBulkEndOffset=1;

  bulkCursor* cursor5 = new bulkCursor;
  cursor5->bulkStartOffset = 0;
  cursor5->bulkEndOffset = 0;
  cursor5->inBulkStartOffset = 0;
  cursor5->inBulkEndOffset=2;

  bulkCursor* cursor6 = new bulkCursor;
  cursor6->bulkStartOffset = 1;
  cursor6->bulkEndOffset = 1;
  cursor6->inBulkStartOffset = 0;
  cursor6->inBulkEndOffset=2;

  bulkCursor* cursor7 = new bulkCursor;
  cursor7->bulkStartOffset = 0;
  cursor7->bulkEndOffset = 1;
  cursor7->inBulkStartOffset = 1;
  cursor7->inBulkEndOffset=1;

  bulkCursor* cursor8 = new bulkCursor;
  cursor8->bulkStartOffset = 0;
  cursor8->bulkEndOffset = 2;
  cursor8->inBulkStartOffset = 1;
  cursor8->inBulkEndOffset=1;

  LOG_INFO("cursor1");
  processPerEvent(cursor1, v);
  LOG_INFO("cursor2");
  processPerEvent(cursor2, v);
  LOG_INFO("cursor3");
  processPerEvent(cursor3, v);
  LOG_INFO("cursor4");
  processPerEvent(cursor4, v);
  LOG_INFO("cursor5");
  processPerEvent(cursor5, v);
  LOG_INFO("cursor6");
  processPerEvent(cursor6, v);
  LOG_INFO("cursor7");
  processPerEvent(cursor7, v);
  LOG_INFO("cursor8");
  processPerEvent(cursor8, v);
  delete cursor1;
  delete cursor2;
  delete cursor3;
  delete cursor4;
  delete cursor5;
  delete cursor6;
  delete cursor7;
  delete cursor8;
  delete v;
}

FileProvenanceElasticTest::~FileProvenanceElasticTest() {}