/*
 * naive_dss.cc
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


#include <dsnet.h>
#include <cassert>


//===============================================================
//
//  Library of methods
//
//===============================================================



void MethodComponent::initialize()
{
    streams = par("streams").longValue();
    method = getParentModule();

    coordinator = check_and_cast<Coordinator*>(method->getSubmodule("coordinator"));
    sites.resize(method->par("sites"));
    for(size_t i=0;i<sites.size();i++)
        sites[i] = check_and_cast<LocalSite*>(method->getSubmodule("site",i));

    streamRecIn = registerSignal("streamRecIn");
    protoMsgSent = registerSignal("protoMsgSent");
    protoMsgRecv = registerSignal("protoMsgRecv");
}




void LocalSite::initialize()
{
    MethodComponent::initialize();

    siteID = par("siteID").longValue();
    assert(this == getSites()[siteID]);

    gate_stream_baseId = gateBaseId("stream");
    gate_coord_in_Id = gate("coord$i")->getId();
    gate_coord_out_Id = gate("coord$o")->getId();

}


void LocalSite::handleMessage(cMessage* mm)
{
    cPacket* m = check_and_cast<cPacket*>(mm);
    int ingate = m->getArrivalGateId();

    if(ingate == gate_coord_in_Id) {
        // message from the coordinator
        emit(protoMsgRecv, m);
        handleCoordinatorMessage(m);
    } else {
        int stream = ingate-gate_stream_baseId;
        assert(stream>=0 && stream < streams);

        emit(streamRecIn,1);
        StreamMessage* sm = check_and_cast<StreamMessage*>(m);

        handleStreamMessage(stream, sm);
    }
    if(m->getOwner() == this)
        dropAndDelete(m);
}


void LocalSite::sendCoordinator(cPacket* m)
{
    emit(protoMsgSent, m);
    send(m, gate_coord_out_Id);
}




void Coordinator::initialize()
{
    MethodComponent::initialize();
    gate_site_in_baseId = gateBaseId("site$i");
    gate_site_out_baseId = gateBaseId("site$o");
}


void Coordinator::handleMessage(cMessage* mm)
{
    cPacket* m = check_and_cast<cPacket*>(mm);
    int s = m->getArrivalGateId()-gate_site_in_baseId;
    assert(s>=0 && s < numSites());

    handleSiteMessage(s, m);
    if(m->getOwner() == this)
        delete m;
}


void Coordinator::sendSite(int s, cPacket* m)
{
    assert(s>=0 && s<numSites());

    send(m, gate_site_out_baseId + s);
}



void DataSource::initialize()
{
    streams = par("streams").longValue();
    sites = par("sites").longValue();
    local_streams = streams*sites;

    local_stream_baseId = gateBaseId("local_stream");
}


void DataSource::emitRecord(StreamMessage* m, int site, int stream)
{
    send(m, local_stream_baseId + site*streams + stream);
}



void LocalStream::initialize()
{
    streamID = par("streamID").longValue();
    siteID = par("siteID").longValue();

    source_gateId = gate("source")->getId();
    stream_gateId = gate("stream")->getId();
}


void LocalStream::emitRecord(StreamMessage *msg)
{
    send(msg, stream_gateId);
}



//===============================================================
//
//  Library of simple DataSource, LocalStream,
//  LocalSite and Coordinator implementations
//
//  These are mostly useful for testing.
//
//===============================================================




/////////////////////////////////////////////////////////////////
Define_Module(StreamBroadcaster);


void StreamBroadcaster::initialize()
{
    streams = par("streams").longValue();
    sites = par("sites").longValue();
    methods = par("methods").longValue();
    local_streams = streams*sites;

    stream_baseId = gateBaseId("stream");
    methstream_baseId = gateBaseId("methstream");
}


void StreamBroadcaster::handleMessage(cMessage* m)
{
    int agate = m->getArrivalGateId() - stream_baseId;

    for(int i=0;i<methods;i++) {
        cMessage* sendmsg = (i==methods-1)? m : m->dup();
        send(sendmsg, i*local_streams + agate + methstream_baseId);
    }
}




/////////////////////////////////////////////////////////////////
Define_Module(SimpleSyntheticDataSource);


void SimpleSyntheticDataSource::initialize()
{
    DataSource::initialize();

    domainSize = par("domainSize").longValue();
    totalStreamLength = par("totalStreamLength").longValue();

    EV_INFO << "Data source initialized" << endl;

    count = 0;

    send_self();
}

void SimpleSyntheticDataSource::finish()
{
    cancelAndDelete(pending);
}

void SimpleSyntheticDataSource::send_self()
{
    if(count >= totalStreamLength) {
        pending = nullptr;
        return;
    }
    pending = new StreamMessage();
    double interval = par("sendInterval").doubleValue();

    pending->setUpdate(INSERT);
    pending->setValue(intuniform(0, domainSize-1));
    pending->setBitLength(64);

    scheduleAt(simTime()+interval, pending);
    count++;
}

void SimpleSyntheticDataSource::handleMessage(cMessage *msg)
{
    // ignore messages sent by the data source
    StreamMessage* m = dynamic_cast<StreamMessage*>(msg);
    assert(m!=nullptr);

    // sent to a uniformly chosen local stream
    emitRecord(m, intuniform(0,sites-1), intuniform(0, streams-1));

    // schedule next self-message
    send_self();
}


////////////////////////////////////////////////////////////////////
Define_Module(PropagatingLocalStream);




void PropagatingLocalStream::handleMessage(cMessage *msg)
{
    emitRecord(check_and_cast<StreamMessage*>(msg));
}




//////////////////////////////////////////////////////
Define_Module(NaiveLocalSite);


void NaiveLocalSite::handleStreamMessage(int i, StreamMessage* m)
{
    sendCoordinator(m);
}


void NaiveLocalSite::handleCoordinatorMessage(cPacket* m)
{
    // this will fail if a message is received by the coordinator
    assert(0);
}




////////////////////////////////////////////////////////
Define_Module(NaiveCoordinator);


void NaiveCoordinator::handleSiteMessage(int s, cPacket *msg)
{
    // TODO - Generated method body
    EV_INFO << "Received message from site "
            << dynamic_cast<NaiveLocalSite*>(msg->getSenderModule())->getSiteID()
            << endl;
}
