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

#ifndef FILEPROVENANCEXATTRBUFFERTABLE_H
#define FILEPROVENANCEXATTRBUFFERTABLE_H

#include "Cache.h"
#include "Utils.h"
#include "XAttrTable.h"

struct FPXAttrBufferPK {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;
  int mInodeLogicalTime;

  FPXAttrBufferPK(Int64 inodeId, Int8 ns, std::string name, int inodeLogicalTime) {
    mInodeId = inodeId;
    mNamespace = ns;
    mName = name;
    mInodeLogicalTime = inodeLogicalTime;
  }

  std::string to_string() {
    std::stringstream out;
    out << mInodeId << "-" << std::to_string(mNamespace) << "-" << mName << "-" << mInodeLogicalTime;
    return out.str();
  }
};

struct FPXAttrVersionsK {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;

  FPXAttrVersionsK(Int64 inodeId, Int8 ns, std::string name) {
    mInodeId = inodeId;
    mNamespace = ns;
    mName = name;
  }

  std::string to_string() {
    std::stringstream out;
    out << mInodeId << "-" << std::to_string(mNamespace) << "-" << mName;
    return out.str();
  }
};

struct FPXAttrBufferRow {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;
  int mInodeLogicalTime;
  std::string mValue;

  std::string to_string(){
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "InodeId = " << mInodeId << std::endl;
    stream << "Namespace = " << (int)mNamespace << std::endl;
    stream << "Name = " << mName << std::endl;
    stream << "InodeLogicalTime = " << mInodeLogicalTime << std::endl;
    stream << "Value = " << mValue << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }

  FPXAttrBufferPK getPK() {
    return FPXAttrBufferPK(mInodeId, mNamespace, mName, mInodeLogicalTime);
  }
};

typedef std::vector <boost::optional<FPXAttrBufferPK> > FPXAttrBKeys;

struct ProvCoreEntry {
  FPXAttrBufferPK key;
  FPXAttrBufferRow value;
  int upToLogicalTime;

  ProvCoreEntry(FPXAttrBufferPK mKey, FPXAttrBufferRow mValue, int mLogicalTime) :
          key(mKey), value(mValue), upToLogicalTime(mLogicalTime) {}
};

struct ProvCore {
  ProvCoreEntry* core1;
  ProvCoreEntry* core2;

  ProvCore(ProvCoreEntry* provCore) : core1(provCore) {}
};

class ProvCoreCache {
public:
  ProvCoreCache(int lru_cap, const char* prefix) : mProvCores(lru_cap, prefix) {}
  /* for each inode we keep to cached values core1 and core2 and they are ordered core1 < core2
  * we do this, in the hope we get a nicer transition we the core changes but we might still get some out of order operations (using old core1)
  * each core is used for an interval of logical times...
  * we do not know the upper bound until we get the next core,
  * so the upToLogicalTime increments as we find from db that we should use same core
  */
  void add(FPXAttrBufferRow value, int opLogicalTime) {
    FPXAttrBufferPK key = value.getPK();
    if(mProvCores.contains(key.mInodeId)) {
      ProvCore provCore = mProvCores.get(key.mInodeId).get();
      //case {new} - <> -> <new>
      if (provCore.core1 == nullptr) {
        //no core defined
        provCore.core1 = new ProvCoreEntry(key, value, opLogicalTime);
        return;
      }
      //update core usage for upTo
      if (provCore.core1->key.mInodeLogicalTime == key.mInodeLogicalTime
          && provCore.core1->upToLogicalTime < opLogicalTime) {
        provCore.core1->upToLogicalTime = opLogicalTime;
        return;
      }
      //case {new, 1, 2?} - <1> -> <new, 1> or <1,2> -> <new, 1>
      if (provCore.core1->key.mInodeLogicalTime > key.mInodeLogicalTime) {
        //evict 2 if necessary(not null)
        if (provCore.core2 != nullptr) {
          delete provCore.core2;
        }
        provCore.core2 = provCore.core1;
        provCore.core1 = new ProvCoreEntry(key, value, opLogicalTime);
        return;
      }
      //holds: core1->key.mInodeLogicalTime < key.mInodeLogicalTime
      //case {1,new} - <1> -> <1,new>
      if (provCore.core2 == nullptr) {
        provCore.core2 = new ProvCoreEntry(key, value, opLogicalTime);
        return;
      }
      //update core usage for upTo
      if (provCore.core2->key.mInodeLogicalTime == key.mInodeLogicalTime
          && provCore.core2->upToLogicalTime < opLogicalTime) {
        provCore.core2->upToLogicalTime = opLogicalTime;
        return;
      }
      //case {1,2,new} - <1,2> -> <2,new>
      if (provCore.core2->key.mInodeLogicalTime < key.mInodeLogicalTime) {
        delete provCore.core1;
        provCore.core1 = provCore.core2;
        provCore.core2 = new ProvCoreEntry(key, value, opLogicalTime);
      } //case {1,new,2} - <1,2> -> <new,2>
      else {
        delete provCore.core1;
        provCore.core1 = new ProvCoreEntry(key, value, opLogicalTime);
      }
    } else {
      ProvCore core1(new ProvCoreEntry(key, value, opLogicalTime));
      mProvCores.put(key.mInodeId, core1);
    }
  }

  boost::optional<FPXAttrBufferRow> get(Int64 inodeId, int logicalTime) {
    boost::optional<ProvCore> provCore = mProvCores.get(inodeId);
    if(provCore) {
      if(provCore.get().core1 != nullptr
         && provCore.get().core1->key.mInodeLogicalTime <= logicalTime && logicalTime <= provCore.get().core1->upToLogicalTime) {
        return provCore.get().core1->value;
      }
      if(provCore.get().core2 != nullptr
         && provCore.get().core2->key.mInodeLogicalTime <= logicalTime && logicalTime <= provCore.get().core1->upToLogicalTime) {
        return provCore.get().core2->value;
      }
    }
    return boost::none;
  }
private:
  Cache<Int64, ProvCore> mProvCores;
};

class FPCCache : public ProvCoreCache {
public:

  FPCCache(int lru_cap, const char* prefix) : ProvCoreCache(lru_cap, prefix) {
  }
};

typedef CacheSingleton<FPCCache> FProvCoreCache;

class FileProvenanceXAttrBufferTable : public DBTable<FPXAttrBufferRow> {

public:
  public:

  FileProvenanceXAttrBufferTable(int lru_cap) : DBTable("hdfs_file_provenance_xattrs_buffer") {
    addColumn("inode_id");
    addColumn("namespace");
    addColumn("name");
    addColumn("inode_logical_time");
    addColumn("value");
    FProvCoreCache::getInstance(lru_cap, "FileProvCore");
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

  FPXAttrBufferRow get(Ndb* connection, Int64 inodeId, Int8 ns, std::string name, int inodeLogicalTime) {
    AnyMap a;
    a[0] = inodeId;
    a[1] = ns;
    a[2] = name;
    a[3] = inodeLogicalTime;
    return DBTable<FPXAttrBufferRow>::doRead(connection, a);
  }

  boost::optional<FPXAttrBufferRow> get(Ndb* connection, FPXAttrBufferPK key) {
    FPXAttrBufferRow row = get(connection, key.mInodeId, key.mNamespace, key.mName, key.mInodeLogicalTime);
    if (readCheckExists(key, row)) {
      return row;
    } else {
      return boost::none;
    }
  }

  std::vector<FPXAttrBufferRow> get(Ndb* connection, FPXAttrVersionsK key) {
    AnyMap a;
    a[0] = key.mInodeId;
    a[1] = key.mNamespace;
    a[2] = key.mName;
    return DBTable<FPXAttrBufferRow>::doRead(connection, "xattr_versions", a, key.mInodeId);
  }

  private:
  inline static bool readCheckExists(FPXAttrBufferPK key, FPXAttrBufferRow row) {
    return key.mInodeId == row.mInodeId && key.mNamespace == row.mNamespace 
    && key.mName == row.mName && key.mInodeLogicalTime == row.mInodeLogicalTime;
  }
};
#endif /* FILEPROVENANCEXATTRBUFFERTABLE_H */