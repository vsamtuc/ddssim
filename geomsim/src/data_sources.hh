/*
 * data_sources.hh
 *
 *  Created on: Oct 11, 2016
 *      Author: vsam
 */

//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program.  If not, see http://www.gnu.org/licenses/.
//

#ifndef DATA_SOURCES_HH_
#define DATA_SOURCES_HH_


#include <cstdio>
#include <string>

#include "dsnet.h"


using std::string;


class FileDataSource : public DataSource
{
    cMessage* notifier;
protected:
    int buffer;
    string filepath;

    void initialize() override;
    void finish() override;
    void handleMessage(cMessage* msg) override;

    struct rec {
        simtime_t tstamp;
        uint site;
        uint stream;
        StreamMessage* msg;
    };

    /*
     * Read up to bufsize rec into buffer, return number
     * of recs read. If 0 is returned, the stream is exhausted.
     *
     * The recs are stored in increasing timestamp order in the
     * buffer.
     */
    virtual int readMessages(rec* buffer, int bufsize) = 0;

    virtual ~FileDataSource();
};



#endif /* DATA_SOURCES_HH_ */
