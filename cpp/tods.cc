
#include <iostream>

#include "method.hh"
#include "tods.hh"


using std::cout;
using std::endl;
using namespace dds;
using namespace dds::tods;


/************************************
 *
 *  TODS method
 *
 ************************************/


tods::network::network(const projection& _proj, double _theta)
: proj(_proj), theta(_theta)
{
	k = CTX.metadata().source_ids().size();

	setup();

	// callback to process new record
	on(START_RECORD, [&](){ process_record(); });
}


tods::network::~network()
{
}


void tods::network::process_record()
{
	const dds_record& rec = CTX.stream_record();
	nodes[rec.hid]->update(rec.sid, rec.key, rec.sop);
}


/************************************
 *
 *  TODS coordinator
 *
 ************************************/

coordinator::coordinator(network* m)
: process(m)
{
	for(stream_id sid : CTX.metadata().stream_ids())
		stream_state[sid] = new coord_stream_state(m->proj);
}


coordinator::~coordinator()
{
	for(auto i : stream_state)
		delete i.second;
}



/************************************
 *
 *  TODS node
 *
 ************************************/

node::node(network* m, source_id hid)
: local_site(m, hid), coord(this, m->hub)
{
	for(stream_id sid : CTX.metadata().stream_ids())
		stream_state[sid] = new node_stream_state(m->proj, m->theta, m->k);
}

void node::setup_connections()
{
	coord.connect( ((network*)_net)->hub );
}

node::~node()
{
	for(auto i : stream_state)
		delete i.second;	
}


void node::update(stream_id sid, key_type key, stream_op op)
{
	auto nss = stream_state[sid];
	// add update
	nss->update(key, (op==stream_op::INSERT)? 1.0 : -1.0);
	// check local condition
	if(! nss->local_condition()) {
		coord.update(site_id(), sid, *nss);
		nss->flush();
	}
}

