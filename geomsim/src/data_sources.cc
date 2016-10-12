/*
 * data_sources.cc
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

#include <cassert>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <cerrno>

#include "data_sources.hh"


using std::max;



void FileDataSource::initialize()
{
    DataSource::initialize();

    buffer = par("buffer").longValue();
    assert(buffer>0);

    filepath = par("filepath").stringValue();

    // allocate and dispatch the notifier
    notifier = new cMessage();
    scheduleAt(0, notifier);
}



void FileDataSource::handleMessage(cMessage* msg)
{
    assert(msg==notifier);
    rec BUFFER[buffer];
    size_t buflen = readMessages(BUFFER, buffer);
    if(buflen>0) {
        simtime_t maxts;
        for(uint i=0;i<buflen;i++) {
            simtime_t ts = BUFFER[i].tstamp;
            maxts = (i==0)? ts : max(ts, maxts);
            double delay = (ts < simTime())? 0.0 : (ts-simTime()).dbl();
            emitRecordDelayed(BUFFER[i].msg, BUFFER[i].site, BUFFER[i].stream, delay);
        }
        scheduleAt(maxts, notifier);
    }
}


void FileDataSource::finish()
{

}

FileDataSource::~FileDataSource()
{
    cancelAndDelete(notifier);
}



//--------------------------------------
// READERS
//--------------------------------------



class CrawdadDataSource : public FileDataSource
{
    FILE* stream;

    struct record {
        char siteString[100];
        char day[100];
        char moment[100];
        char parent[100];
        unsigned int aid;
        char state[100];
        unsigned int shortRet;
        unsigned int longRet;
        int strength;
        int quality;
        char mac[100];
        char classId[100];
        unsigned int srcPkts;
        unsigned int srcOct;
        unsigned int srcErrPkts;
        unsigned int srcErrOct;
        unsigned int dstPkts;
        unsigned int dstOct;
        unsigned int dstErrPkts;
        unsigned int dstErrOct;
        unsigned int dstMaxRetryErr;
        char ip[100];

        void readNextRequest(FILE* stream) {

            int rc = fscanf(stream, "%s %s %s %s %d %s %d %d %d %d %s %s %d %d %d %d %d %d %d %d %d %s\n",
                    siteString, day, moment, parent, &(this->aid), state,
                    &(this->shortRet), &(this->longRet), &(this->strength), &(this->quality), mac,
                    classId, &(this->srcPkts), &(this->srcOct), &(this->srcErrPkts), &(this->srcErrOct),
                    &(this->dstPkts), &(this->dstOct), &(this->dstErrPkts), &(this->dstErrOct), &(this->dstMaxRetryErr),
                    ip);
            if(rc != 22) {
                int errsv = errno;
                throw cRuntimeError("In %s: rc=%d\n errno=%d. %s\n", __FUNCTION__, rc, errsv, strerror(errsv));
            }
        }

        inline int stream() const { return (siteString[0]=='L') ? 0 : 1; }

        inline int site() const { return aid-29; }

        inline int value() const { return shortRet; }

        static inline const simtime_t date2time(int yr, int mo, int day, int hr, int min, int sec)
        {
            return sec + 60l*min + 3600l*hr + 86400l*(365l*yr+31l*mo+day-31l);
        }

        const simtime_t dataset_base_tstamp = date2time(2, 7, 20, 0, 0, 0);

        simtime_t tstamp() const {
            int rc;
            int yr, mo, da, hr, min, sec;

            // read the day
            rc = sscanf(day, "%d-%d-%d", &yr, &mo, &da);
            if(rc!=3) {
                int errsv = errno;
                throw cRuntimeError("In %s: rc=%d\n errno=%d. %s\n", __FUNCTION__, rc, errsv, strerror(errsv));
            }

            // read the hour
            rc = sscanf(moment, "%d:%d:%d", &hr, &min, &sec);
            if(rc!=3) {
                int errsv = errno;
                throw cRuntimeError("In %s: rc=%d\n errno=%d. %s\n", __FUNCTION__, rc, errsv, strerror(errsv));
            }

            return date2time(yr,mo,da,hr,min,sec) - dataset_base_tstamp;
        }

    };

public:


    virtual void initialize() override {
        FileDataSource::initialize();

        if(streams<1 || streams >2)
            throw new cRuntimeError("Crawdad datasets support only one or two streams, %d requested", streams);

        stream = fopen(filepath.c_str(), "r");
        if(stream == nullptr) {
            throw new cRuntimeError("Could not open data file %s", filepath.c_str());
        }

    }

    virtual int readMessages(rec* buffer, int bufsize) override {
        int i;
        simtime_t told = simTime();
        for(i=0; i< bufsize; i++) {
            if(feof(stream)) break;
            record r;
            r.readNextRequest(stream);
            buffer[i].site = r.site() % sites;
            buffer[i].stream = r.stream() % streams;

            simtime_t tstamp = r.tstamp();
            if(tstamp < told) {
                EV_ERROR << "At time " << told << " tstamp of record is " << tstamp << " = " << r.day << " " << r.moment <<  endl;
            }

            buffer[i].tstamp = tstamp ; //tstamp < now ? now : tstamp;
            told = tstamp;

            buffer[i].msg = new StreamMessage();
            buffer[i].msg->setValue(r.value());
            buffer[i].msg->setUpdate(INSERT);
            buffer[i].msg->setBitLength(64);
        }
        return i;
    }

    ~CrawdadDataSource() { fclose(stream); }

};


Define_Module(CrawdadDataSource);






