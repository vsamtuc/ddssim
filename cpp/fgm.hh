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
using std::vector;
using namespace agms;
using namespace dds;

using dds::qtype;

template <qtype QType>
struct coordinator;

template <qtype QType>
struct node;

template <qtype QType>
struct node_proxy;


template <qtype QType>
struct network 
	: 	star_network<network<QType> , coordinator<QType> , node<QType> >, 
		component
{
	typedef coordinator<QType> coordinator_t;
	typedef node<QType> node_t;
	typedef network<QType> network_t;
	typedef star_network<network_t, coordinator_t, node_t> star_network_t;

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


//////////////////////////////////
// 
// Query, state, safezone
//
//////////////////////////////////



template <qtype QType>
struct coordinator : process
{
	typedef coordinator<QType> coordinator_t;
	typedef node<QType> node_t;
	typedef node_proxy<QType> node_proxy_t;
	typedef network<QType> network_t;

	typedef typename continuous_query<QType>::query_type query_type;

	proxy_map<node_proxy_t, node_t> proxy;

	//
	// protocol stuff
	//
	query_type query;	// current query state
	size_t total_updates;	// number of stream updates received

	bool in_naive_mode;		// when true, use the naive safezone

	size_t k;				// number of sites

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
	
	coordinator(network_t* nw, const projection& proj, double beta); 
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
	void trace_round(sketch& newE);
	// this is used to print the state of nodes
	void print_state();
	
};



template <qtype QType>
struct coord_proxy : remote_proxy< coordinator<QType> >
{
	typedef coordinator<QType> coordinator_t;
	REMOTE_METHOD(coordinator_t, threshold_crossed);
	coord_proxy(process* c) : remote_proxy< coordinator<QType> >(c) { }
};



template <qtype QType>
struct node : local_site
{
	typedef coordinator<QType> coordinator_t;
	typedef node<QType> node_t;
	typedef node_proxy<QType> node_proxy_t;
	typedef network<QType> network_t;
	typedef coord_proxy<QType> coord_proxy_t;

	typedef typename continuous_query<QType>::safezone_type safezone_type;
	typedef typename continuous_query<QType>::state_vector_type state_vector_type;
	typedef typename continuous_query<QType>::drift_vector_type drift_vector_type;

	int num_sites;			// number of sites

	safezone_type szone;	// safezone object

	double minzeta; 		// minimum value of zeta so far
	double zeta_l, zeta_u;          // zetas of lower and upper bounds
	double zeta;			// current zeta = min(zeta_l, zeta_u)

	double zeta_0;			// start value for discretization, equal to zeta at last reset_bitweight()
	double zeta_quantum;	// discretization for bitweight, set by reset_bitweight()
	int bitweight;			// equal to number of bits sent since last reset_bitweight()

	drift_vector_type U;	// drift vector
	size_t update_count;	// number of updates in drift vector

	state_vector_type dS;		// the sketch of all updates over a round
	size_t round_local_updates; // number of local stream updates since last reset

	coord_proxy_t coord;

	node(network_t* net, source_id hid, const projection& proj, double beta)
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

template <qtype QType>
struct node_proxy : remote_proxy< node<QType> >
{
	typedef node<QType> node_t;

	REMOTE_METHOD(node_t, reset);
	REMOTE_METHOD(node_t, set_safezone);
	REMOTE_METHOD(node_t, reset_bitweight);
	REMOTE_METHOD(node_t, get_drift);
	REMOTE_METHOD(node_t, set_drift);
	REMOTE_METHOD(node_t, get_zeta);
	REMOTE_METHOD(node_t, get_zeta_lu);
	node_proxy(process* p) : remote_proxy< node<QType> >(p) {}
};

} // end namespace gm::fgm

}  // end namespace gm


namespace dds{
	
template <>
inline size_t byte_size< gm::fgm::node<qtype::SELFJOIN> *>(gm::fgm::node<qtype::SELFJOIN> * const &) { return 4; }
}



#endif
