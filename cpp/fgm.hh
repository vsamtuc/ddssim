#ifndef __FGM_HH__
#define __FGM_HH__

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
#include "gm.hh"


namespace gm { namespace fgm {

using std::cout;
using std::endl;
using namespace agms;
using namespace dds;

struct coordinator;
struct node;

struct network : star_network<network, coordinator, node>, reactive
{
	stream_id sid;
	projection proj;
	double beta;

	network(const string& name, stream_id, const projection& _proj, double _beta);

	network(const string& _name, stream_id _sid, depth_type D, index_type L, double _beta) 
	: network(_name, _sid, projection(D,L), _beta) 
	{ }

	void process_record();
	void process_init();
	void output_results();
};


extern gm::component_type<network> fgm_comptype;



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
	
	coordinator(network* nw, const projection& proj, double beta); 
	~coordinator();

	inline network* net() { return static_cast<network*>(host::net()); }

	void setup_connections() override;

	// load the warmup dataset
	void warmup();

	//
	// State transitions (except for threshold)
	//

	// remote call on host violation
	oneway threshold_crossed(sender<node> ctx, int delta_bitw);

	void start_round();   // initialize a new round
	void finish_round();  // finish current round and start new one
	
	
	void start_subround(double total_zeta);    // initialize a new subround
	void finish_subround();                    // finish the subround
	void finish_subrounds(double total_zeta);  // try to rebalance (optional)

	//
	// rebalancing
	//

	// This method performs the actual rebalancing and returns the delta_zeta
	// of the rebalanced nodes.
	double rebalance(const set<node_proxy*> B);

	// Returns a rebalancing set two nodes, or empty.
	set<node_proxy*> rebalance_pairs();
	// Returns a rebalancing set of high-h, low stream traffic nodes
	set<node_proxy*> rebalance_light();
	
	// used in rebalancing heuristics: return the vector of zeta_lu for each node
	vector<node_double> compute_hvalue();

	// 
	// model routines
	//
	void compute_model();
	void print_model();

	//
	// this is used to trace the execution of rounds, for debugging or tuning
	//
	void trace_round(sketch& newE);
	// this is used to print the state of nodes
	void print_state();
	
};




struct coord_proxy : remote_proxy<coordinator>
{
	REMOTE_METHOD(coordinator, threshold_crossed);
	coord_proxy(process* c) : remote_proxy<coordinator>(c) { }
};




struct node : local_site
{
	int num_sites;				// number of sites

	safezone szone;	// pointer to the safezone (shared among objects)

	double minzeta; 		// minimum value of zeta so far
	double zeta_l, zeta_u;          // zetas of lower and upper bounds
	double zeta;			// current zeta = min(zeta_l, zeta_u)

	double zeta_0;			// start value for discretization, equal to zeta at last reset_bitweight()
	double zeta_quantum;	// discretization for bitweight, set by reset_bitweight()
	int bitweight;			// equal to number of bits sent since last reset_bitweight()

	isketch U;				// drift vector
	size_t update_count;	// number of updates in drift vector

	sketch dS;				// the sketch of all updates over a round
	size_t round_local_updates; // number of local stream updates since last reset

	coord_proxy coord;

	node(network* net, source_id hid, const projection& proj, double beta)
	: local_site(net, hid), 
		U(proj), update_count(0),
		dS(proj), round_local_updates(0),
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
		zeta = minzeta = szone.prepare_inc(U, zeta_l, zeta_u);
		assert(zeta==szone.zeta_E);

		// reset for the first subround
		reset_bitweight(szone.zeta_E/2);

		// reset round statistics
		dS = 0.0;
		round_local_updates = 0;
	}

// Called to upgrade the safezone from naive to full
	int set_safezone(const safezone& newsz) {
		// reset the safezone object
		szone = newsz;
		double newzeta = szone.prepare_inc(U, zeta_l, zeta_u);
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

	double get_zeta_lu() {
		return zeta_l - zeta_u;
	}

	// called at the start a new subrounds
	oneway reset_bitweight(double Z)
	{
		zeta_0 = zeta;
		zeta_quantum = Z;
		bitweight = 0;
	}

	compressed_sketch get_drift() {
		return compressed_sketch { U, update_count };
	}

	double set_drift(compressed_sketch newU) {
		(sketch&)U = newU.sk;
		update_count = newU.updates;
		double old_zeta = zeta;
		zeta = szone.prepare_inc(U, zeta_l, zeta_u);
		return zeta-old_zeta;
	}


};

struct node_proxy : remote_proxy<node>
{
	REMOTE_METHOD(node, reset);
	REMOTE_METHOD(node, set_safezone);
	REMOTE_METHOD(node, reset_bitweight);
	REMOTE_METHOD(node, get_drift);
	REMOTE_METHOD(node, set_drift);
	REMOTE_METHOD(node, get_zeta);
	REMOTE_METHOD(node, get_zeta_lu);
	node_proxy(process* p) : remote_proxy<node>(p) {}
};

} // end namespace gm::fgm

}  // end namespace gm


namespace dds{
	
template <>
inline size_t byte_size<gm::fgm::node*>(gm::fgm::node* const &) { return 4; }
}



#endif
