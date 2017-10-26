
#include <algorithm>

#include "results.hh"
#include "binc.hh"
#include "gm.hh"

using namespace dds;
using namespace dds::gm;

using binc::print;
using binc::elements_of;


/*
	Implements the traditional Gemoetric Method and its variants
*/


/*********************************************
	node
*********************************************/

void node::update_stream() 
{
	assert(CTX.stream_record().hid == site_id());

	U.update(CTX.stream_record().key, num_sites * (CTX.stream_record().upd));
	update_count++;
	round_local_updates++;

	zeta = szone(U, zeta_l, zeta_u);

	if(zeta <= 0)
		coord.local_violation(this);
}


void node::setup_connections()
{
	num_sites = coord.proc()->k;
}


/*********************************************
	coordinator
*********************************************/


// initialize a new round
void coordinator::start_round()
{
	// compute current parameters from query

	if(query.zeta_E < k*sqrt(query.E.width())) {
		if(!in_naive_mode)
			print("SWITCHING TO NAIVE MODE stream_count=",CTX.stream_count());
		in_naive_mode = true;
	} else {
		if(in_naive_mode)
			print("SWITCHING TO FULL MODE stream_count=",CTX.stream_count());
		in_naive_mode = false;
	}

	has_naive.assign(k, in_naive_mode);

	for(auto p : proxy) {
		//variation
		if(!in_naive_mode)
			p.second->reset(safezone(&query.safe_zone, &query.E, total_updates, query.zeta_E));
		else
			p.second->reset(safezone(query.zeta_E));
	}
}	



// remote call on host violation
oneway coordinator::local_violation(sender<node> ctx)
{
	node* n = ctx.value;

	/* 
		In this function, we try to rebalance. If this fails, we
		restart the round.
	 */
	if(! in_naive_mode && k>1) {
		// attempt to rebalance		
		// get rebalancing set
		rebalance_random(& proxy[n]);
	} else {
		B.clear();
		Bcompl.clear();
		for(auto p : proxy) 
			Bcompl.insert(p.second);

		Ubal = 0.0;
		Ubal_updates = 0;
		Ubal_admissible = false;

		finish_round();
	}

}



void coordinator::rebalance_random(node_proxy* lvnode)
{
	B.clear();
	Bcompl.clear();

	B.insert(lvnode);
	compressed_sketch csk = lvnode->get_drift();
	Ubal = csk.sk;
	Ubal_updates = csk.updates;
	Ubal_admissible = false;
	assert(query.safe_zone(query.E + Ubal) <= 0.0);

	// find a balancing set
	vector<node_proxy*> nodes;
	nodes.reserve(k);
	for(auto p : proxy) {
		node_proxy* n = p.second;
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
			compressed_sketch csk = n->get_drift();
			Ubal += csk.sk;
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


void coordinator::rebalance()
{
	Ubal /= B.size();

	assert(query.safe_zone(query.E + Ubal) > 0);

	compressed_sketch skbal { Ubal, Ubal_updates };

	for(auto n : B) {
		n->set_drift(skbal);
	}	

	for(auto n : node_ptr)
		assert(n->zeta > 0);
}



// initialize a new round
void coordinator::finish_round()
{
	// collect all data
	for(auto n : Bcompl) {
		compressed_sketch csk = n->get_drift();
		Ubal += csk.sk;
		Ubal_updates += csk.updates;
	}
	Ubal /= (double)k;

#if 1
	trace_round(Ubal);
#endif

	// new round
	query.update_estimate(Ubal);
	start_round();
}


void coordinator::trace_round(sketch& newE)
{

	// report
	static int skipper = 0;
	skipper = (skipper+1)%1;

	if( !in_naive_mode || skipper==0 ) {

		sketch Enext = query.E + newE;
		double zeta_Enext = query.safe_zone(Enext);

		valarray<size_t> round_updates((size_t)0, k);

		for(size_t i=0; i<k; i++) 
		{
			auto ni = node_ptr[i];
			round_updates[i] += ni->round_local_updates;
		}

		// check the value of the next E wrt this safe zone
		double norm_dE = norm_L2(newE);

		print("GM Finish round : round updates=",round_updates.sum()," naive=",in_naive_mode, 
			"zeta_E=",query.zeta_E, "zeta_E'=", zeta_Enext, zeta_Enext/query.zeta_E,
			"||dE||=", norm_dE, norm_dE/query.zeta_E, 
			//"minzeta_min=", minzeta_total, minzeta_total/(k*query.zeta_E),
			//"minzeta_min/zeta_E=",minzeta_min/query.zeta_E,
			" QEst=", query.Qest,
			" time=", (double)CTX.stream_count() / CTX.metadata().size() );

		// print elements
		print("                  : S= ",elements_of(round_updates));

		//emit(RESULTS);
	}
	
}





void coordinator::warmup()
{
	sketch dE(net()->proj);

	for(auto&& rec : CTX.warmup) {
		if(rec.sid == net()->sid) 
			dE.update(rec.key, rec.upd);
	}
	query.update_estimate(dE);
}


void coordinator::setup_connections()
{
	proxy.add_sites(net());
	for(auto n : net()->sites) {
		node_index[n.second] = node_ptr.size();
		node_ptr.push_back(n.second);
	}
	k = proxy.size();
}


coordinator::coordinator(network* nw, const projection& proj, double beta)
: 	process(nw), proxy(this), 
	query(beta, proj), total_updates(0), 
	in_naive_mode(true), k(proxy.size()),
	Qest_series("gm_qest", "%.10g", [&]() { return query.Qest;} ),
	Ubal(proj)
{  
}

coordinator::~coordinator()
{
}

/*********************************************

	network

*********************************************/


gm::network::network(stream_id _sid, const projection& _proj, double _beta)
: 	star_network<network, coordinator, node>(CTX.metadata().source_ids()),
	sid(_sid), proj(_proj), beta(_beta) 
{
	set_name("GM");
	
	setup(proj, beta);
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

void gm::network::process_record()
{
	const dds_record& rec = CTX.stream_record();
	if(rec.sid==sid) 
		sites[rec.hid]->update_stream();		
}

void gm::network::process_init()
{
	// let the coordinator initialize the nodes
	CTX.timeseries.add(hub->Qest_series);
	hub->warmup();
	hub->start_round();
}


void gm::network::output_results()
{
	//network_comm_results.netname = "GM2";

	network_comm_results.max_error = beta;
	network_comm_results.sites = sites.size();
	network_comm_results.streams = 1;
	network_comm_results.local_viol = 0;
	network_comm_results.fill_columns(this);
	network_comm_results.emit_row();

	network_host_traffic.output_results(this);
	network_interfaces.output_results(this);
}
