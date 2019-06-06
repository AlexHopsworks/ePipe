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
 * File:   FileProvenanceXAttrBufferTable.h
 * Author: Alexandru Ormenisan <aaor@kth.se>
 *
 */

#ifndef FILEPROVENANCEXATTRBUFFERTABLE_H
#define FILEPROVENANCEXATTRBUFFERTABLE_H

#include "XAttrTable.h"

struct FPXAttrBufferPK {
  Int64 mInodeId;
  Int8 mNamespace;
  string mName;
  int mInodeLogicalTime;

  FPXAttrBufferPK(Int64 inodeId, Int8 nameSpace, string name, int inodeLogicalTime) {
    mInodeId = inodeId;
    mNamespace = nameSpace;
    mName = name;
    mInodeLogicalTime = inodeLogicalTime;
  }

  string to_string() {
    stringstream out;
    out << mInodeId << "-" << mNamespace << "-" << mName << "-" << mInodeLogicalTime;
    return out.str();
  }
};

struct FPXAttrBufferRow {
  Int64 mInodeId;
  Int8 mNamespace;
  string mName;
  int mInodeLogicalTime;
  string mValue;

  string to_string(){
    stringstream stream;
    stream << "-------------------------" << endl;
    stream << "InodeId = " << mInodeId << endl;
    stream << "Namespace = " << (int)mNamespace << endl;
    stream << "Name = " << mName << endl;
    stream << "InodeLogicalTime = " << mInodeLogicalTime << endl;
    stream << "Value = " << mValue << endl;
    stream << "-------------------------" << endl;
    return stream.str();
  }
};

typedef vector<boost::optional<FPXAttrBufferPK> > FPXAttrBKeys;

class FileProvenanceXAttrBufferTable : public DBTable<FPXAttrBufferRow> {

public:
  public:

  FileProvenanceXAttrBufferTable() : DBTable("hdfs_file_provenance_xattrs_buffer") {
    addColumn("inode_id");
    addColumn("namespace");
    addColumn("name");
    addColumn("inode_logical_time");
    addColumn("value");
  }

  FPXAttrBufferRow getRow(NdbRecAttr* values[]) {
    FPXAttrBufferRow row;
    row.mInodeId = values[0]->int64_value();
    row.mNamespace = values[1]->int8_value();
    row.mName = get_string(values[2]);
    row.mInodeLogicalTime = values[3]->int32_value();
    row.mValue = get_string(values[4]);
    return row;
  }

  FPXAttrBufferRow get(Ndb* connection, Int64 inodeId, Int8 ns, string name, int inodeLogicalTime) {
    AnyMap a;
    a[0] = inodeId;
    a[1] = ns;
    a[2] = name;
    a[3] = inodeLogicalTime;
    return DBTable<FPXAttrBufferRow>::doRead(connection, a);
  }

  boost::optional<FPXAttrBufferRow> get(Ndb* connection, FPXAttrBufferPK key) {
    FPXAttrBufferRow row = get(connection, key.mInodeId, key.mNamespace, key.mName, key.mInodeLogicalTime);
    if(readCheckExists(key, row)) {
      return row;
    } else {
      return boost::none;
    }
  }
  void cleanBuffer(Ndb* connection, FPXAttrBKeys& pks) {
    start(connection);
    for (FPXAttrBKeys::iterator it = pks.begin(); it != pks.end(); ++it) {
      boost::optional<FPXAttrBufferPK> pk = *it;
      if(pk) {
        AnyMap a;
        a[0] = pk.get().mInodeId;
        a[1] = pk.get().mNamespace;
        a[2] = pk.get().mName;
        a[3] = pk.get().mInodeLogicalTime;
        
        doDelete(a);
        LOG_DEBUG("Delete xattr buffer row: " << pk.get().to_string());
      }
    }
    end();
  }

  private:
  static bool readCheckExists(FPXAttrBufferPK key, FPXAttrBufferRow row) {
    return key.mInodeId == row.mInodeId && key.mNamespace == row.mNamespace 
    && key.mName == row.mName && key.mInodeLogicalTime == row.mInodeLogicalTime;
  }
};
#endif /* FILEPROVENANCEXATTRBUFFERTABLE_H */