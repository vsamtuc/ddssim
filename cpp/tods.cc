
#include <iostream>

#include "method.hh"
#include "tods.hh"
#include "results.hh"

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
	on(RESULTS, [&](){ output_results(); });
}


tods::network::~network()
{
}


void tods::network::process_record()
{
	const dds_record& rec = CTX.stream_record();
	nodes[rec.hid]->update(rec.sid, rec.key, rec.sop);
}

double tods::network::maximum_error() const
{
	double eps = proj.epsilon();
	return eps + pow(1+eps,2.0)*(2.* theta + theta*theta);
}

void tods::network::output_results()
{
	comm_results.netname = "TODS";
	comm_results.max_error = maximum_error();
	comm_results.sites = k;
	comm_results.streams = CTX.metadata().stream_ids().size();
	comm_results.local_viol = 0;
	this->comm_results_fill_in();
	comm_results.emit_row();
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

oneway coordinator::update(source_id hid, stream_id sid, 
	const node_stream_state& nss)
{
	stream_state[sid]->Etot += nss.dE.sk;
	return oneway();
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

node_stream_state::node_stream_state(projection proj, double theta, size_t k) 
: E(proj), dE(proj), delta_updates(0), 
	norm_E_2(0.0), norm_dE_2(0.0),
	theta_2_over_k(theta*theta/k)
{ }


void node_stream_state::update(key_type key, double freq) 
{
	// update with 
	dE.update(key, freq);
	
	double delta_2 = dot(dE.delta);

	Vec tmp = dE.sk[dE.idx];

	double dnorm_dE_2 = 2.*dot(tmp,  dE.delta) 
			- delta_2;
	
	norm_dE_2 += dnorm_dE_2;

	tmp = E[dE.idx];
	double dnorm_E_2 = dnorm_dE_2 + 2.*dot(tmp, dE.delta);
	norm_E_2 += dnorm_E_2;

	delta_updates++;
}

/// check local condition
bool node_stream_state::local_condition() const
{
	return norm_dE_2 < theta_2_over_k * norm_E_2;
}

/// flush dE to E
void node_stream_state::flush() 
{
	E += dE.sk;
	dE.sk = 0.0;
	delta_updates = 0;
	norm_E_2 = pow(norm_L2(E), 2);
	norm_dE_2 = 0.0;
}

size_t node_stream_state::byte_size() const 
{
	size_t E_size = dds::byte_size(dE.sk);
	size_t Raw_size = sizeof(dds_record)*delta_updates;
	return std::min(E_size, Raw_size);
}

