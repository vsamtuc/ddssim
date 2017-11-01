
#ifndef __GM_HH__
#define __GM_HH__

#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <cmath>
#include <vector>

#include "dds.hh"
#include "dsarch.hh"
#include "agms.hh"
#include "safezone.hh"
#include "method.hh"

using std::cout;
using std::endl;
using namespace agms;


namespace dds { namespace gm {


struct coordinator;
struct node;

struct network : star_network<network, coordinator, node>, reactive
{
	stream_id sid;
	projection proj;
	double beta;

	network(stream_id, const projection& _proj, double _beta);

	network(stream_id _sid, depth_type D, index_type L, double _beta) 
	: network(_sid, projection(D,L), _beta) 
	{
		
	}

	void process_record();
	void process_init();
	void output_results();
};



struct node_proxy;

using std::vector;

typedef tuple<node_proxy*,double>  node_double;


struct coordinator : process
{
	proxy_map<node_proxy, node> proxy;

	//
	// protocol stuff
	//
	selfjoin_query query;	// current query state
	size_t total_updates;	// number of stream updates received

	bool in_naive_mode;		// when true, use the naive safezone

	size_t k;				// number of sites

	// index the nodes
	map<node*, size_t> node_index;
	vector<node*> node_ptr;

	// protocol related
	vector<bool> has_naive;

	// report the series 
	computed<double> Qest_series;


	coordinator(network* nw, const projection& proj, double beta); 
	~coordinator();

	inline network* net() { return static_cast<network*>(host::net()); }

	void setup_connections() override;

	// load the warmup dataset
	void warmup();

	// initialize a new round
	void start_round();
	void finish_round();
	
	// initialize a new subround
	void start_subround(double total_zeta);
	void finish_subrounds(double total_zeta);

	//
	// rebalancing
	//

	// used during rebalancing

	set<node_proxy*> B;			// initialized by local_violation(), 
								// updated by rebalancing algo

	set<node_proxy*> Bcompl;	// complement of B, updated by rebalancing algo

	sketch Ubal;				// contains \sum_{i\in B} U_i
	size_t Ubal_updates;		// Ubal updates

	bool Ubal_admissible;		// contains zeta(Ubal)>0

	size_t round_total_B;           // total size of B over round
  

	// This method performs the actual rebalancing and returns the delta_zeta
	// of the rebalanced nodes.
	void rebalance();

	// Returns a rebalancing set two nodes, or empty.
	void rebalance_random(node_proxy* lvnode);
	void rebalance_random_limits(node_proxy* lvnode);
	
	//
	// this is used to trace the execution of rounds, for debugging or tuning
	//
	void trace_round(sketch& newE);

	// remote call on host violation
	oneway local_violation(sender<node> ctx);

	// statistics
	size_t num_rounds;				 // total number of rounds
	size_t num_subrounds;			 // total number of subrounds
	size_t sz_sent;					 // total safe zones sent
	size_t total_rbl_size; 			 // sum of all rebalance sets
};




struct coord_proxy : remote_proxy<coordinator>
{
	REMOTE_METHOD(coordinator, local_violation);
	coord_proxy(process* c) : remote_proxy<coordinator>(c) { }
};


/**
	This is a site implementation for the classic Geometric Method protocol.

 */
struct node : local_site
{
	int num_sites;				// number of sites

	safezone szone;	// pointer to the safezone (shared among objects)

	double zeta_l, zeta_u;          // zetas of lower and upper bounds
	double zeta;			// current zeta = min(zeta_l, zeta_u)

	isketch U;				// drift vector
	size_t update_count;	// number of updates in drift vector

	size_t round_local_updates; // number of local stream updates since last reset

	coord_proxy coord;

	node(network* net, source_id hid, const projection& proj, double beta)
	: local_site(net, hid), 
		U(proj), update_count(0),
		coord( this )
	{ 
		coord <<= net->hub;
	}

	void setup_connections() override;

	void update_stream();

	//
	// Remote methods
	//

	oneway reset(const safezone& newsz) { 
		// reset the safezone object
		szone = newsz;

		// reset the drift vector
		(sketch&)U = 0.0;
		update_count = 0;
		zeta = szone.prepare_inc(U, zeta_l, zeta_u);
		assert(zeta==szone.zeta_E);

		// reset round statistics
		round_local_updates = 0;
	}


	compressed_sketch get_drift() {
		return compressed_sketch { U, update_count };
	}

	void set_drift(compressed_sketch newU) {
		(sketch&)U = newU.sk;
		update_count = newU.updates;
		zeta = szone.prepare_inc(U, zeta_l, zeta_u);
		assert(zeta>0);
	}


};

struct node_proxy : remote_proxy<node>
{
	REMOTE_METHOD(node, reset);
	REMOTE_METHOD(node, get_drift);
	REMOTE_METHOD(node, set_drift);
	node_proxy(process* p) : remote_proxy<node>(p) {}
};

} // end namespace dds::gm


template <>
inline size_t byte_size<gm::node*>(gm::node  * const & ) { return 4; }


}  // end namespace dds




#endif
