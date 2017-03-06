#ifndef __TODS_HH__
#define __TODS_HH__

#include <functional>

#include "dds.hh"
#include "mathlib.hh"
#include "agms.hh"
#include "dsarch.hh"
#include "method.hh"


namespace dds { namespace tods {

using namespace agms;

class network;

/**
	State of a stream, used by coordinator
  */
struct coord_stream_state
{
	stream_id sid;
	sketch Etot;		// the sum of all E in node states
	computed<double> curest_series;	

	coord_stream_state(stream_id _sid, projection proj)
	: 	sid(_sid), Etot(proj), 
		curest_series("tods_", "%.10g", [&](){ return dot_est(Etot); })
	{ 
		CTX.timeseries.add(curest_series); 
	}
};


/**
	State of a stream, used a node.
  */
struct node_stream_state
{
	sketch E;				// the last sketch sent to the coordinator
	isketch dE;				// collects the new updates
	size_t delta_updates; 	// counts the updates in dE

	double norm_X_2;		// dynamically maintained ||E+dE||**2
	double norm_dE_2;		// dynamically maintained ||dE||**2
	double theta_2_over_k;	// equal to theta**2/k, used in local condition

	node_stream_state(projection proj, double theta, size_t k);

	node_stream_state(const node_stream_state&)=delete;
	node_stream_state& operator=(const node_stream_state&)=delete;

	/// add an update to the state
	void update(key_type key, double freq);

	/// check local condition
	bool local_condition() const;

	/// flush dE to E
	void flush();

	size_t byte_size() const;
};


/**
	Coordinator for the TODS method
  */
struct coordinator : process
{
	map<stream_id, coord_stream_state*> stream_state;

	inline network* net() const { return (network*) host::net(); }

	coordinator(network*);
	~coordinator();

	/// Remote method
	oneway update(source_id hid, stream_id sid, const node_stream_state& nss);
};


struct coordinator_proxy : remote_proxy<coordinator>
{
	REMOTE_METHOD(coordinator, update);

	coordinator_proxy(process* owner, coordinator* coord)
	: remote_proxy<coordinator>(owner)
	{ }
};



/**
	Nodes for the TODS method
  */
struct node : local_site
{
	/// map from stream_id to stream_state
	map<stream_id, node_stream_state*> stream_state;

	coordinator_proxy coord;

	inline network* net() const { return (network*) host::net(); }

	node(network*, source_id);
	~node();

	void setup_connections() override;

	void update(stream_id sid, key_type key, counter_type upd);
};


/**
	The main method object
  */
struct network 
	: star_network<network, coordinator, node>, reactive
{
	set<stream_id> streams;
	projection proj;
	double theta;
	size_t k;

	network(const projection& proj, double theta, const set<stream_id>& streams);
	network(depth_type D, index_type L, double theta, const set<stream_id>& streams)
	: network(projection(D,L), theta, streams)
	{ }	

	network(const projection& proj, double theta);
	network(depth_type D, index_type L, double theta)
	: network(projection(D,L), theta)
	{ }

	void process_warmup();
	void process_record();
	void output_results();

	double maximum_error() const;


	~network();
};




} } // end namespace dds


#endif