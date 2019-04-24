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
  JanusGraphBase(string janusgraph_addr);

  void addData(Bulk<Keys> data);
  
  void shutdown();
  
  virtual ~JanusGraphBase();

protected:
  string getJanusGraphUrl();
  virtual bool parseResponse(string response);

};

template<typename Keys>
JanusGraphBase<Keys>::JanusGraphBase(string janusgraph_addr)
: TimedRestBatcher<Keys>(janusgraph_addr, 0, 0) {
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
    LOG_ERROR("Parsing for error1");
    if (!d.Parse<0>(response.c_str()).HasParseError()) {
      LOG_ERROR("Parsing for error2");
      if (d.HasMember("message")) {
        // LOG_ERROR("Parsing for error3");
        const rapidjson::Value &msg = d["message"];
        string s1 = msg.GetString();
        string s2 = "no gremlin script supplied";
        if(s1 == s2) {
          stringstream error;
          error << msg.GetString();
          LOG_ERROR("Janusgraph got errors: " << error.str());
          return false;
        }
      }
      if (d.HasMember("Exception-Class")) {
        const rapidjson::Value &errorClass = d["Exception-Class"];
        const rapidjson::Value &errorMsg = d["message"];
        LOG_ERROR("Error:");
        stringstream error;
        error << errorClass.GetString() << ":" << errorMsg.GetString();
        LOG_ERROR("Janusgraph got errors: " << error.str());
        return false;
      } else {
        return true;
      }
    } else {
      LOG_ERROR(" ES got json error (" << d.GetParseError() << ") while parsing (" << response << ")");
      return false;
    }
  } catch (std::exception &e) {
    LOG_ERROR(e.what());
    return false;
  }
}

template<typename Keys>
JanusGraphBase<Keys>::~JanusGraphBase() {

}
#endif //EPIPE_JANUSGRAPHBASE_H
