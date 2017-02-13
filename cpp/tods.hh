#ifndef __TODS_HH__
#define __TODS_HH__

#include <vector>

#include "dds.hh"
#include "mathlib.hh"
#include "agms.hh"
#include "dsarch.hh"
#include "method.hh"


namespace dds { namespace tods {

using std::vector;
using namespace agms;

class network;

/**
	State of a stream, used by coordinator
  */
struct coord_stream_state
{
	sketch Etot;		// the sum of all E in node states

	coord_stream_state(projection proj)
	: Etot(proj)
	{ }
};


/**
	State of a stream, used a node.
  */
struct node_stream_state
{
	sketch E;				// the last sketch sent to the coordinator
	incremental_sketch dE;	// collects the new updates
	size_t delta_updates; 	// counts the updates in dE

	double norm_E_2;		// dynamically maintained ||E||**2
	double norm_dE_2;		// dynamically maintained ||dE||**2

	double theta_2_over_k;	// equal to theta**2/k, used in local condition

	node_stream_state(projection proj, double theta, size_t k);

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
	map<stream_id, node_stream_state*> stream_state;
	coordinator_proxy coord;

	node(network*, source_id);
	~node();

	void setup_connections() override;

	void update(stream_id sid, key_type key, stream_op op);
};


/**
	The main method object
  */
struct network 
	: star_network<network, coordinator, node>, reactive
{
	projection proj;
	double theta;
	size_t k;

	network(const projection& proj, double theta);
	network(depth_type D, index_type L, double theta)
	: network(projection(D,L), theta)
	{ }

	void process_record();
	void output_results();

	double maximum_error() const;


	~network();
};




} } // end namespace dds


#endif