
#include <algorithm>
#include <boost/range/adaptors.hpp>

#include "results.hh"
#include "binc.hh"
#include "sgm.hh"

using namespace dds;
using namespace gm;
using namespace gm::sgm;


using binc::print;
using binc::elements_of;


/*
	Implements the traditional, Set-based Geometric Method and its variants
*/


/*********************************************
	node
*********************************************/

template <qtype QType>
void node<QType>::update_stream() 
{
	assert(CTX.stream_record().hid == site_id());

	delta_vector delta = Q.delta_update(U, CTX.stream_record());
	if(delta.size()==0) return;

	update_count++;
	round_local_updates++;

	zeta = szone(delta, U);

	if(zeta <= 0)
		coord.local_violation(this);
}


template <qtype QType>
void node<QType>::setup_connections()
{
	num_sites = coord.proc()->k;
}


/*********************************************
	coordinator
*********************************************/


// initialize a new round
template <qtype QType>
void coordinator<QType>::start_round()
{

	for(auto n : net()->sites) {
		sz_sent ++;
		proxy[n].reset(safezone(&query));
	}

	round_total_B = 0;
	num_rounds++;
	num_subrounds++;
}	



// remote call on host violation
template <qtype QType>
oneway coordinator<QType>::local_violation(sender<node_t> ctx)
{
	node_t* n = ctx.value;

	/* 
		In this function, we try to rebalance. If this fails, we
		restart the round.
	 */
	if(k>1) {
		// attempt to rebalance		
		// get rebalancing set
		rebalance_random_limits(n);
	} else {
		B.clear();
		Bcompl.clear();
		for(auto n : node_ptr) 
			Bcompl.insert(n);

		Ubal = 0.0;
		Ubal_updates = 0;
		Ubal_admissible = false;

		finish_round();
	}

}



template <qtype QType>
void coordinator<QType>::rebalance_random(node_t* lvnode)
{
	B.clear();
	Bcompl.clear();

	B.insert(lvnode);
	compressed_state csk = proxy[lvnode].get_drift();
	Ubal = csk.vec;
	Ubal_updates = csk.updates;
	Ubal_admissible = false;
	assert(query.safe_zone(query.E + Ubal) <= 0.0);

	// find a balancing set
	vector<node_t*> nodes;
	nodes.reserve(k);
	for(auto n : node_ptr) {
		if(B.find(n) == B.end())
			nodes.push_back(n);
	}
	assert(nodes.size()==k-1);

	// permute the order
	random_shuffle(nodes.begin(), nodes.end());
	assert(nodes.size()==k-1);
	assert(B.size()==1);
	assert(Bcompl.empty());

	double zbal = query.safe_zone( query.E + Ubal/((double) B.size()) );
	for(auto n : nodes) {
		if(Ubal_admissible) {
			Bcompl.insert(n);
		} else {
			B.insert(n);
			compressed_state cs = proxy[n].get_drift();
			Ubal += csk.vec;
			Ubal_updates += csk.updates;
			zbal = query.safe_zone( query.E + Ubal/((double) B.size()) );
			Ubal_admissible =  (zbal>0.0) ? true : false;
		}
	}
	assert(B.size()+Bcompl.size() == k);

	// if it is a partial balancing set, rebalance, else finish round
	if(! Bcompl.empty()) {
		//print("       GM Rebalancing ",B.size()," sites");
		assert(Ubal_admissible);
		assert(zbal > 0.0);
		assert(B.size()>1);
		rebalance();
	} else {
		//print("       GM Rebalancing failed, finishing round");
		finish_round();
	}
}

/*
  In order to reduce the cost of over-rebalancing, we impose 
  ad-hoc limits to 
  (a) the size of the rebalance set be up to ceil(k+2/2)
  k  max|B|
  2  2
  3  3
  4  3
  5  4
  6  4
  ...

  (b) \sum |B|  over a round to be <= k
  ... 
 */
template <qtype QType>
void coordinator<QType>::rebalance_random_limits(node_t* lvnode)
{
	B.clear();
	Bcompl.clear();

	B.insert(lvnode);
	compressed_state cs = proxy[lvnode].get_drift();
	Ubal = cs.vec;
	Ubal_updates = cs.updates;
	Ubal_admissible = false;
	assert(query.safe_zone(query.E + Ubal) <= 0.0);

	// find a balancing set
	vector<node_t*> nodes;
	nodes.reserve(k);
	for(auto n : node_ptr) {
		if(B.find(n) == B.end())
			nodes.push_back(n);
	}
	assert(nodes.size()==k-1);

	// permute the order
	random_shuffle(nodes.begin(), nodes.end());
	assert(nodes.size()==k-1);
	assert(B.size()==1);
	assert(Bcompl.empty());

	double zbal = query.safe_zone( query.E + Ubal/((double) B.size()) );
	//
	// The loop computes a rebalancing set B of minimum size, trying
	// in order each of the nodes in the random order selected.
	//
	for(auto n : nodes) {
		if(Ubal_admissible) {
			Bcompl.insert(n);
		} else {
			B.insert(n);
			compressed_state cs = proxy[n].get_drift();
			Ubal += cs.vec;
			Ubal_updates += cs.updates;
			zbal = query.safe_zone( query.E + Ubal/((double) B.size()) );
			Ubal_admissible =  (zbal>0.0) ? true : false;
		}
	}
	assert(B.size()+Bcompl.size() == k);

	// check conditions for finishing round
	bool fin = Bcompl.empty();

	// (a) check limit on |B|
	fin = fin ||  B.size() > (k+3)/2;

	// (b) check limit on  sum|B|
	fin = fin ||  (round_total_B + B.size()) > k;
	
	if(! fin) {
		//print("       GM Rebalancing ",B.size()," sites");
		assert(Ubal_admissible);
		assert(zbal > 0.0);
		assert(B.size()>1);
		rebalance();
	} else {
		//print("       GM Rebalancing failed, finishing round");
		finish_round();
	}
}


template <qtype QType>
void coordinator<QType>::rebalance()
{
	Ubal /= B.size();

	assert(query.safe_zone(query.E + Ubal) > 0);

	compressed_state sbal { Ubal, Ubal_updates };

	for(auto n : B) {
		proxy[n].set_drift(sbal);
	}	

	round_total_B += B.size();
	
	for(auto n : node_ptr)
		assert(n->zeta > 0);

	num_subrounds++; 
	total_rbl_size += B.size();
}



// initialize a new round
template <qtype QType>
void coordinator<QType>::finish_round()
{
	// collect all data
	for(auto n : Bcompl) {
		compressed_state cs = proxy[n].get_drift();
		Ubal += cs.vec;
		Ubal_updates += cs.updates;
	}
	Ubal /= (double)k;

#if 0
	trace_round(Ubal);
#endif

	// new round
	query.update_estimate(Ubal);
	start_round();
}


template <qtype QType>
void coordinator<QType>::trace_round(const Vec& newE)
{
	Vec Enext = query.E + newE;
	double zeta_Enext = query.safe_zone(Enext);

	valarray<size_t> round_updates((size_t)0, k);

	for(size_t i=0; i<k; i++) 
	{
		auto ni = node_ptr[i];
		round_updates[i] += ni->round_local_updates;
	}

	// check the value of the next E wrt this safe zone
	double norm_dE = norm_L2(newE);

	print("GM Finish round : round updates=",round_updates.sum(),
		"zeta_E=",query.zeta_E, "zeta_E'=", zeta_Enext, zeta_Enext/query.zeta_E,
		"||dE||=", norm_dE, norm_dE/query.zeta_E, 
		//"minzeta_min=", minzeta_total, minzeta_total/(k*query.zeta_E),
		//"minzeta_min/zeta_E=",minzeta_min/query.zeta_E,
		" QEst=", query.Qest,
		" time=", (double)CTX.stream_count() / CTX.metadata().size() );

	// print elements
	print("                  : S= ",elements_of(round_updates));	
}





template <qtype QType>
void coordinator<QType>::warmup()
{
	Vec dE(Q.state_vector_size());

	for(auto&& rec : CTX.warmup) 
		Q.update(dE, rec);

	query.update_estimate(dE/(double)k);
}


template <qtype QType>
void coordinator<QType>::setup_connections()
{
	using boost::adaptors::map_values;
	proxy.add_sites(net()->sites);
	for(auto n : net()->sites) {
		node_index[n] = node_ptr.size();
		node_ptr.push_back(n);
	}
	k = node_ptr.size();
}


template <qtype QType>
coordinator<QType>::coordinator(network_t* nw, const continuous_query_t& _Q)
: 	process(nw), proxy(this), 
	Q(_Q), 
	query(Q.beta, Q.proj), 
	total_updates(0), 
	k(0),
	Qest_series(nw->name()+".qest", "%.10g", [&]() { return query.Qest;} ),
	Ubal(0.0, Q.state_vector_size()),
	num_rounds(0), num_subrounds(0), sz_sent(0), total_rbl_size(0)
{  
}

template <qtype QType>
coordinator<QType>::~coordinator()
{
}

/*********************************************

	network

*********************************************/


template <qtype QType>
sgm::network<QType>::network(const string& _name, const continuous_query<QType>& _Q)
: 	star_network_t(CTX.metadata().source_ids()), Q(_Q)
{
	set_name(_name);
	this->set_protocol_name("GM");
	
	this->setup(Q);

	on(START_STREAM, [&]() { 
		process_init(); 
	} );
	on(START_RECORD, [&]() { 
		process_record(); 
	} );
	on(RESULTS, [&](){ 
		output_results();
	});
}

template <qtype QType>
void sgm::network<QType>::process_record()
{
	const dds_record& rec = CTX.stream_record();
	this->source_site(rec.hid)->update_stream();
}

template <qtype QType>
void sgm::network<QType>::process_init()
{
	// let the coordinator initialize the nodes
	CTX.timeseries.add(this->hub->Qest_series);
	this->hub->warmup();
	this->hub->start_round();
}


template <qtype QType>
void sgm::network<QType>::output_results()
{
	//network_comm_results.netname = "GM2";

	network_comm_results.fill_columns(this);
	network_comm_results.emit_row();

	network_host_traffic.output_results(this);
	network_interfaces.output_results(this);

	gm_comm_results.fill(this);
	gm_comm_results.emit_row();
}


gm::p_component_type<network> sgm::sgm_comptype("SGM");

