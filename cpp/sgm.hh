#ifndef __SGM_HH__
#define __SGM_HH__

#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <cmath>
#include <vector>

#include "gm_proto.hh"

using std::cout;
using std::endl;


namespace gm { namespace sgm {

using namespace dds;
using namespace agms;
using std::vector;

struct coordinator;
struct node;
struct node_proxy;


struct network : gm_network<network, coordinator, node >
{
	typedef gm_network<network_t, coordinator_t, node_t> gm_network_t;

	network(const string& _name, continuous_query* Q);
};



struct coordinator : process
{
	typedef coordinator coordinator_t;
	typedef node node_t;
	typedef node_proxy node_proxy_t;
	typedef network network_t;

	//typedef tuple<node_proxy_t*,double>  node_double;

	proxy_map<node_proxy_t, node_t> proxy;

	//
	// protocol stuff
	//
    continuous_query* Q;   		// continuous query
    query_state* query; 		// current query state
 	safezone_func* safe_zone; 	// the safe zone wrapper

	size_t k;					// number of sites

	// index the nodes
	map<node_t*, size_t> node_index;
	vector<node_t*> node_ptr;

	// report the series 
	computed<double> Qest_series;


	coordinator(network_t* nw, continuous_query* _Q); 
	~coordinator();

	inline network_t* net() { return static_cast<network_t*>(host::net()); }
    inline const protocol_config& cfg() const { return Q->config; }

	void setup_connections() override;

	// load the warmup dataset
	void warmup();

	// initialize a new round
	void start_round();
	void finish_round();
	void finish_rounds();
	
	// initialize a new subround
	void start_subround(double total_zeta);
	void finish_subrounds(double total_zeta);

	//
	// rebalancing
	//

	// used during rebalancing

	set<node_t*> B;				// initialized by local_violation(), 
								// updated by rebalancing algo

	set<node_t*> Bcompl;		// complement of B, updated by rebalancing algo

	Vec Ubal;					// contains \sum_{i\in B} U_i
	size_t Ubal_updates;		// Ubal updates
	bool Ubal_admissible;		// contains zeta(Ubal)>0

	size_t round_total_B;       // total size of B over round
  

	// This method performs the actual rebalancing and returns the delta_zeta
	// of the rebalanced nodes.
	void rebalance();

	// Rebalancing strategies (rebalancing set selectors)
	void rebalance_none(node_t* lvnode);
	void rebalance_random(node_t* lvnode);
	void rebalance_random_limits(node_t* lvnode);

	// add the drift vector of a node to Ubal
	void fetch_updates(node_t* n);
	
	//
	// this is used to trace the execution of rounds, for debugging or tuning
	//
	void trace_round(const Vec& newE);

	// remote call on host violation
	oneway local_violation(sender<node_t> ctx);

	// statistics
	size_t num_rounds;				 // total number of rounds
	size_t num_subrounds;			 // total number of subrounds
	size_t sz_sent;					 // total safe zones sent
	size_t total_rbl_size; 			 // sum of all rebalance sets
 	size_t total_updates;		// number of stream updates received
};



struct coord_proxy : remote_proxy< coordinator >
{
	using coordinator_t = coordinator;
	REMOTE_METHOD(coordinator_t, local_violation);
	coord_proxy(process* c) : remote_proxy<coordinator_t>(c) { }
};


/**
	This is a site implementation for the classic Geometric Method protocol.

 */
struct node : local_site
{
	typedef coordinator coordinator_t;
	typedef node node_t;
	typedef node_proxy node_proxy_t;
	typedef network network_t;
	typedef coord_proxy coord_proxy_t;
    typedef continuous_query continuous_query_t;

    continuous_query* Q;   // the query management object
    safezone szone;    // safezone object


	int num_sites;			// number of sites

	double zeta;			// current zeta 

	Vec U;					// drift vector
	size_t update_count;	// number of updates in drift vector

	size_t round_local_updates; // number of local stream updates since last reset

	coord_proxy_t coord;

	node(network_t* net, source_id hid, continuous_query* _Q)
	: local_site(net, hid), Q(_Q),
		U(Q->state_vector_size()), update_count(0),
		coord( this )
	{ 
		coord <<= net->hub;
	}

	void setup_connections() override;

	void update_stream();

	//
	// Remote methods
	//

	// called at the start of a round
	oneway reset(const safezone& newsz); 

	// transfer data to the coordinator
	compressed_state_ref get_drift();

	// set the drift vector (for rebalancing)
	void set_drift(compressed_state_ref newU);

};

struct node_proxy : remote_proxy< node >
{
	typedef node node_t;
	REMOTE_METHOD(node_t, reset);
	REMOTE_METHOD(node_t, get_drift);
	REMOTE_METHOD(node_t, set_drift);
	node_proxy(process* p) : remote_proxy<node_t>(p) {}
};

} // end namespace gm::sgm

}  // end namespace gm


namespace dsarch {
	template<>
	inline size_t byte_size<gm::sgm::node*>(gm::sgm::node* const &) { return 4; }
}


#endif
