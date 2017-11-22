#ifndef __FGM_HH__
#define __FGM_HH__

#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <cmath>
#include <vector>

#include "dds.hh"
#include "dsarch.hh"
#include "method.hh"
#include "gm_proto.hh"

namespace gm { namespace fgm {

using std::cout;
using std::endl;
using std::vector;
using namespace dds;


struct coordinator;

struct node;

struct node_proxy;


struct network 
	: 	star_network<network , coordinator , node>, 
		component
{
	typedef coordinator coordinator_t;
	typedef node node_t;
	typedef network network_t;
	typedef star_network<network_t, coordinator_t, node_t> star_network_t;

	continuous_query* Q;

	network(const string& name, continuous_query* _Q);
	~network();

	void process_record();
	void process_init();
	void output_results();
};


//////////////////////////////////
// 
// Query, state, safezone
//
//////////////////////////////////

// Ball safezone implementation
struct ball_safezone : safezone_func_wrapper
{
	query_state* query;
	ball_safezone(query_state* q) : query(q) { }

	inline double zeta_E() const { return query->zeta_E; }

	virtual void* alloc_incstate() override;
    virtual void free_incstate(void*) override;
    virtual double compute_zeta(void* inc, const delta_vector& dU, const Vec& U) override;
    virtual double compute_zeta(void* inc, const Vec& U) override;
    virtual size_t zeta_size() const override;
};



struct coordinator : process
{
	typedef coordinator coordinator_t;
	typedef node node_t;
	typedef node_proxy node_proxy_t;
	typedef network network_t;

	proxy_map<node_proxy_t, node_t> proxy;

	//
	// protocol stuff
	//

	continuous_query* Q; 		// continuous query

	query_state* query;			// current query state
	ball_safezone* ballsz;		// the compressed ball safezone
	
	size_t total_updates;		// number of stream updates received
	bool in_naive_mode;			// when true, use the naive safezone
	size_t k;					// number of sites

	// index the nodes
	map<node_t*, size_t> node_index;
	vector<node_t*> node_ptr;

	// protocol related
	vector<bool> has_naive;
	vector<int> bitweight, total_bitweight;

	int bit_budget;
	int bit_level;

	// report the series 
	computed<double> Qest_series;

	// statistics
	size_t num_rounds;       // number of rounds
	size_t num_subrounds;    // number of subrounds
	size_t sz_sent;          // safe zones sent
	size_t total_rbl_size; 	 // total size of rebalance sets

	size_t round_sz_sent;    // safezones sent in curren


	// source models
	Vec alpha, beta, gamma;
	vector<bool> md;  // the model's output;
	
	coordinator(network_t* nw, continuous_query* _Q); 
	~coordinator();

	inline network_t* net() { return static_cast<network_t*>(host::net()); }

	void setup_connections() override;

	// load the warmup dataset
	void warmup();

	//
	// State transitions (except for threshold)
	//

	// remote call on host violation
	oneway threshold_crossed(sender<node_t> ctx, int delta_bitw);

	void start_round();   // initialize a new round
	void finish_round();  // finish current round and start new one
	
	
	void start_subround(double total_zeta);    // initialize a new subround
	void finish_subround();                    // finish the subround
	void finish_subrounds(double total_zeta);  // try to rebalance (optional)

	//
	// rebalancing
	//
#if 0
	// This method performs the actual rebalancing and returns the delta_zeta
	// of the rebalanced nodes.
	double rebalance(const set<node_proxy_t*> B);

	// Returns a rebalancing set two nodes, or empty.
	set<node_proxy*> rebalance_pairs();
	// Returns a rebalancing set of high-h, low stream traffic nodes
	set<node_proxy*> rebalance_light();
	
	// used in rebalancing heuristics: return the vector of zeta_lu for each node
	vector<node_double> compute_hvalue();
#endif

	// 
	// model routines
	//
	void compute_model();
	void print_model();

	//
	// this is used to trace the execution of rounds, for debugging or tuning
	//
	void trace_round(const Vec& newE);
	// this is used to print the state of nodes
	void print_state();
	
};



struct coord_proxy : remote_proxy< coordinator >
{
	typedef coordinator coordinator_t;
	REMOTE_METHOD(coordinator_t, threshold_crossed);
	coord_proxy(process* c) : remote_proxy< coordinator_t >(c) { }
};


struct node : local_site
{
	typedef coordinator coordinator_t;
	typedef node node_t;
	typedef node_proxy node_proxy_t;
	typedef network network_t;
	typedef coord_proxy coord_proxy_t;

	continuous_query* Q;	// the query management object
	safezone szone;			// safezone object

	int num_sites;			// number of sites

	double minzeta; 		// minimum value of zeta so far
	double zeta;			// current zeta 

	double zeta_0;			// start value for discretization, equal to zeta at last reset_bitweight()
	double zeta_quantum;	// discretization for bitweight, set by reset_bitweight()
	int bitweight;			// equal to number of bits sent since last reset_bitweight()

	Vec U;					// drift vector
	size_t update_count;	// number of updates in drift vector

	Vec dS;						// the sketch of all updates over a round
	size_t round_local_updates; // number of local stream updates since last reset

	coord_proxy_t coord;

	node(network_t* net, source_id hid, continuous_query* _Q)
	: 	local_site(net, hid), Q(_Q),
		U(Q->state_vector_size()), update_count(0),
		dS(Q->state_vector_size()), round_local_updates(0),
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
		U = 0.0;
		update_count = 0;
		zeta = minzeta = szone(U);
		// reset for the first subround		
		reset_bitweight(zeta);

		// reset round statistics
		dS = 0.0;
		round_local_updates = 0;
	}

// Called to upgrade the safezone from naive to full
	int set_safezone(const safezone& newsz) {
		// reset the safezone object
		szone = newsz;
		double newzeta = szone(U);
		assert(newzeta >= zeta);
		zeta = newzeta;

		// reset the bit count
		int delta_bitweight = floor((zeta_0-zeta)/zeta_quantum) - bitweight;
		bitweight += delta_bitweight;
		assert(delta_bitweight <= 0);
		return delta_bitweight;
	}
		
	double get_zeta() {
		return zeta;
	}


	// called at the start a new subrounds
	oneway reset_bitweight(double Z)
	{
		zeta_0 = zeta;
		zeta_quantum = Z;
		bitweight = 0;
	}

	compressed_state get_drift() {
		return compressed_state { U, update_count };
	}

	double set_drift(compressed_state newU) {
		U = newU.vec;
		update_count = newU.updates;
		double old_zeta = zeta;
		zeta = szone(U);
		return zeta-old_zeta;
	}


};

struct node_proxy : remote_proxy< node >
{
	typedef node node_t;

	REMOTE_METHOD(node_t, reset);
	REMOTE_METHOD(node_t, set_safezone);
	REMOTE_METHOD(node_t, reset_bitweight);
	REMOTE_METHOD(node_t, get_drift);
	REMOTE_METHOD(node_t, set_drift);
	REMOTE_METHOD(node_t, get_zeta);
	node_proxy(process* p) : remote_proxy< node_t >(p) {}
};

} // end namespace gm::fgm

}  // end namespace gm


namespace dds{
	
template <>
inline size_t byte_size< gm::fgm::node *>
	(gm::fgm::node * const &) { return 4; }

}



#endif
