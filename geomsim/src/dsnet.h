/*
 * naive_dss.h
 *
 *  Created on: Sep 28, 2016
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

#ifndef __GEOMSIM_NAIVE_H_
#define __GEOMSIM_NAIVE_H_

#define COMPILETIME_LOGLEVEL LOGLEVEL_TRACE

#include <vector>
#include <deque>
#include <omnetpp.h>
#include "stream_message_m.h"

using namespace omnetpp;

using std::vector;
using std::deque;



/********************************************************************
 *
 * Local sites and coordinators: library of components
 *
 *
 *
 ********************************************************************
 */


class Coordinator;
class LocalSite;

/**
 * Base class for LocalSite and Coordinator. This class basically defines
 * attributes and methods that enable discovery and communication.
 */
class MethodComponent :  public  cSimpleModule
{
protected:
    int streams;

    cModule* method;
    Coordinator* coordinator;
    vector<LocalSite*> sites;

    void initialize() override;
public:
    // signals
    simsignal_t streamRecIn;
    simsignal_t protoMsgSent, protoMsgRecv;

    inline Coordinator* getCoordinator() const { return coordinator; }
    inline const vector<LocalSite*>& getSites() const { return sites; }
    inline int numSites() const { return sites.size(); }
};


class Coordinator;


/*
 * The LocalSite class is the base for local site classes.
 *
 * It contains method-generic code for communication, as well
 * as statistics instrumentation.
 */
class LocalSite : public MethodComponent
{
protected:
    int siteID;

    int gate_stream_baseId;
    int gate_coord_in_Id, gate_coord_out_Id;


    void initialize() override;
    void handleMessage(cMessage*) override;

    // handle a stream input to the method
    virtual void handleStreamMessage(int stream, StreamMessage* m) = 0;

    // handle a coordinator message
    virtual void handleCoordinatorMessage(cPacket*) = 0;

    void sendCoordinator(cPacket* m);

public:
    inline int getSiteID() const { return siteID; }
};



/*
 * The Coordinator class is the base for coordinator classes.
 *
 * It contains method-generic code for communication, as well
 * as statistics instrumentation.
 */
class Coordinator : public MethodComponent
{
protected:
    int gate_site_in_baseId, gate_site_out_baseId;

    void initialize() override;
    void handleMessage(cMessage*) override;

    virtual void handleSiteMessage(int s, cPacket*) = 0;
    void sendSite(int s, cPacket*);
};



/**
 * A StreamBroadcaster is a simple multiplexer whose job is to copy
 * each local stream to the M methods of the simulation.
 */
class StreamBroadcaster : public cSimpleModule
{
    int streams, sites, methods;
    int local_streams;
    int stream_baseId;
    int methstream_baseId;
protected:
    virtual void initialize() override;
    virtual void handleMessage(cMessage *msg) override;
};


/**
 * A DataSource is an emitter of stream records. It takes care of all
 * streams related.
 */
class DataSource : public cSimpleModule
{
protected:
    int streams, sites;
    int local_streams;
    int local_stream_baseId;

    virtual void initialize() override;
    virtual void emitRecord(StreamMessage* m, int site, int stream);

};



class LocalStream : public cSimpleModule
{
    int streamID, siteID;

  protected:
    int source_gateId, stream_gateId;
    virtual void initialize() override;
    virtual void emitRecord(StreamMessage* m);
    virtual void handleMessage(cMessage* m) override;

    virtual void handleStreamRecord(StreamMessage*) =0;

  public:
    inline int getStream() const { return streamID; }
    inline int getSite() const { return siteID; }
};




/**************************************************************
 *
 * Simple implementations
 *
 */




/**
 * This class generates synthetic stream data, following the uniform
 * distribution.
 */
class SimpleSyntheticDataSource : public DataSource
{
    int64_t domainSize;
    int64_t totalStreamLength;
    StreamMessage* pending=0;

    int64_t count;  // used to count sent messages

    // Schedule a self-message with the next stream record to send
    void send_self();

protected:
    virtual void initialize() override;
    virtual void finish() override;
    virtual void handleMessage(cMessage *msg) override;
};


/**
 *  This class implements a simple local stream object which simply propagates
 *  its input to its output. It is intended to be usable as the base class of other
 *  local stream implementations.
 */
class PropagatingLocalStream : public LocalStream
{
  protected:
    virtual void handleStreamRecord(StreamMessage* sm) override;
};



/*
 * Sliding window LocalStream.
 */
class SlidingWindowLocalStream : public LocalStream
{
protected:
    SimTime windowTime;
    simsignal_t wsize_signal;
    unsigned long wsize;

    virtual void initialize() override;
    virtual void handleStreamRecord(StreamMessage* sm) override;
};


/*
 * Sliding buffer LocalStream
 */
class SlidingBufferLocalStream : public LocalStream
{
protected:
    size_t windowSize;
    simsignal_t winterval_signal;
    deque<StreamMessage*> buffer;
    SimTime wlast;

    virtual void initialize() override;
    virtual void finish() override;
    virtual void handleStreamRecord(StreamMessage* sm) override;
};





/**
 * This local site simply forwards its stream messages to the coordinator.
 */
class NaiveLocalSite : public LocalSite
{
protected:
    virtual void handleStreamMessage(int stream, StreamMessage* m) override;
    virtual void handleCoordinatorMessage(cPacket* m) override;
};



/**
 * This coordinator just consumes the messages.
 */
class NaiveCoordinator : public Coordinator
{
  protected:
    virtual void handleSiteMessage(int s, cPacket *msg) override;
};




#endif



