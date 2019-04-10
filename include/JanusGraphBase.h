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
 * File:   JanusGraphBase.h
 * Author: Alexandru Ormenisan<aaor@kth.se>
 *
 */

#ifndef EPIPE_JANUSGRAPHBASE_H
#define EPIPE_JANUSGRAPHBASE_H

#include "TimedRestBatcher.h"

template<typename Keys>
class JanusGraphBase : public TimedRestBatcher<Keys> {
public:
  JanusGraphBase(string janusgraph_addr, int time_to_wait_before_inserting, int bulk_size);

  void addData(Bulk<Keys> data);
  
  void shutdown();
  
  virtual ~JanusGraphBase();

protected:
  string getJanusGraphUrl();
  virtual bool parseResponse(string response);

};

template<typename Keys>
JanusGraphBase<Keys>::JanusGraphBase(string janusgraph_addr, int time_to_wait_before_inserting, int bulk_size)
: TimedRestBatcher<Keys>(janusgraph_addr, time_to_wait_before_inserting, bulk_size) {
}

template<typename Keys>
string JanusGraphBase<Keys>::getJanusGraphUrl() {
  string str = this->mEndpointAddr;
  return str;
}

template<typename Keys>
bool JanusGraphBase<Keys>::parseResponse(string response) {
  try {
    rapidjson::Document d;
    if (!d.Parse<0>(response.c_str()).HasParseError()) {
      if (d.HasMember("errors")) {
        const rapidjson::Value &bulkErrors = d["errors"];
        if (bulkErrors.IsBool() && bulkErrors.GetBool()) {
          const rapidjson::Value &items = d["items"];
          stringstream errors;
          for (rapidjson::SizeType i = 0; i < items.Size(); ++i) {
            const rapidjson::Value &obj = items[i];
            for (rapidjson::Value::ConstMemberIterator itr = obj.MemberBegin(); itr != obj.MemberEnd(); ++itr) {
              const rapidjson::Value & opObj = itr->value;
              if (opObj.HasMember("error")) {
                const rapidjson::Value & error = opObj["error"];
                if (error.IsObject()) {
                  const rapidjson::Value & errorType = error["type"];
                  const rapidjson::Value & errorReason = error["reason"];
                  errors << errorType.GetString() << ":" << errorReason.GetString();
                } else if (error.IsString()) {
                  errors << error.GetString();
                }
                errors << ", ";
              }
            }
          }
          string errorsStr = errors.str();
          LOG_ERROR(" ES got errors: " << errorsStr);
          return false;
        }
      } else if (d.HasMember("error")) {
        const rapidjson::Value &error = d["error"];
        if (error.IsObject()) {
          const rapidjson::Value & errorType = error["type"];
          const rapidjson::Value & errorReason = error["reason"];
          LOG_ERROR(" ES got error: " << errorType.GetString() << ":" << errorReason.GetString());
        } else if (error.IsString()) {
          LOG_ERROR(" ES got error: " << error.GetString());
        }
        return false;
      }
    } else {
      LOG_ERROR(" ES got json error (" << d.GetParseError() << ") while parsing (" << response << ")");
      return false;
    }

  } catch (std::exception &e) {
    LOG_ERROR(e.what());
    return false;
  }
  return true;
}

template<typename Keys>
JanusGraphBase<Keys>::~JanusGraphBase() {

}
#endif //EPIPE_JANUSGRAPHBASE_H
