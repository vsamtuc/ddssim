
#include <algorithm>

#include "results.hh"
#include "binc.hh"
#include "agm.hh"

using namespace dds;
using namespace dds::agm;

using binc::print;
using binc::elements_of;

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
	if(zeta<minzeta) minzeta = zeta;

	// if(fabs(zeta-last_zeta)>=szone.threshold) {
	int bwnew = floor((zeta_0-zeta)/zeta_quantum);
	int dbw = bwnew - bitweight;
	if(dbw>0) {
		bitweight = bwnew;
		coord.threshold_crossed(this, dbw);
	}
}


void node::setup_connections()
{
	num_sites = coord.proc()->k;
}


/*********************************************
	coordinator
*********************************************/



// invariant sum(zeta) > Threshold

// initialize a new round
void coordinator::start_round()
{
	// compute current parameters from query
	bitweight.assign(k, 0);
	total_bitweight.assign(k,0);

	bit_level = 1;
	bit_budget = k;

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

		// normal case
		//p.second->reset(safezone(&query.safe_zone, &query.E, total_updates, query.zeta_E, P*query.zeta_E));
	}
}	


void coordinator::start_subround(double total_zeta)
{
	bit_budget = k;
	bitweight.assign(k,0);
	for(auto p : proxy) {
		p.second->reset_bitweight(total_zeta/(2.0*k));
	}	
}


// remote call on host violation
oneway coordinator::threshold_crossed(sender<node> ctx, int delta_bits)
{
	const int MAX_LEVEL = 300;
	node* n = ctx.value;

	assert(delta_bits>0);
	size_t nid = node_index[n];

	bitweight[nid] += delta_bits;
	total_bitweight[nid] += delta_bits;

	bit_budget -= delta_bits;

	if(bit_budget < 0) {

		if(bit_level < MAX_LEVEL) {
			// continue the aprroximation of zeta
			bit_level++;

			double total_zeta = 0.0;
			for(auto p : proxy) {
				total_zeta += p.second->get_zeta();
			}

			if(total_zeta < query.zeta_E * 0.05 ) {
				// we are done!
				//assert(total_zeta<0);
				finish_subrounds(total_zeta);
			} else {
				start_subround(total_zeta);
			}


		} else {
			finish_round();
		}
	}

}

double coordinator::rebalance(const set<node_proxy*> B)
{
	assert(B.size());

	sketch Ubal(query.E.proj);
	size_t upd = 0;

	for(auto n : B) {
		compressed_sketch csk = n->get_drift();
		Ubal += csk.sk;
		upd += csk.updates;
	}
	Ubal /= B.size();

	compressed_sketch skbal { Ubal, upd };

	double delta_zeta = 0.0;
	for(auto n : B) {
		delta_zeta += n->set_drift(skbal);
	}
	
	print("           Rebalancing ",B.size(),", gained ", delta_zeta,
	      " %=", (delta_zeta)/query.zeta_E );

	return delta_zeta;
}


set<node_proxy*> coordinator::rebalance_pairs()
{
	vector< node_double > hvalue = compute_hvalue();

	auto Cmp = [](const node_double& a, const node_double& b) -> bool {
		return get<1>(a)<get<1>(b);
	};

	using std::min_element;
	using std::max_element;
	auto nmin = min_element(hvalue.begin(), hvalue.end(), Cmp);
	auto nmax = max_element(hvalue.begin(), hvalue.end(), Cmp);

	double minh = get<1>(*nmin);
	double maxh = get<1>(*nmax);

	set<node_proxy*> B;

	if(  minh*maxh < 0) {
		double g = min(-minh, maxh);

		if(g > 0.1*query.zeta_E) {
			B.insert(get<0>(*nmin));
			B.insert(get<0>(*nmax));
		} else {
			print("       No rebalancing, max gain=",g,
			      " with g/zeta_E=", g/query.zeta_E, " zeta_E=",query.zeta_E);
		}
	}
	return B;
}


set<node_proxy*> coordinator::rebalance_light()
{
	set<node_proxy*> B;

	// Compute new E
	sketch newU(query.E.proj);
	for(auto ni : node_ptr) {
		newU += ni->U;
	}
	newU /= k;

	sketch Enext = query.E + newU;
	double zeta_Enext = query.safe_zone(Enext);

	if(zeta_Enext > 0.05 * query.zeta_E)
		for(auto p : proxy) B.insert(p.second);

	return B;
}


vector<node_double> coordinator::compute_hvalue()
{
	vector<node_double> hvalue;
	hvalue.reserve(k);

	for(auto p : proxy) {
		double h = p.second->get_zeta_lu();
		hvalue.push_back(make_tuple(p.second, h));
	}

	return hvalue;
}


void coordinator::finish_subrounds(double total_zeta)
{
	/* 
		In this function, we try to rebalance.
	 */

	if(! in_naive_mode && k>1) {
		// attempt to rebalance		
		// get rebalancing set
		set<node_proxy*> B;
		// B = rebalance_pairs();
		// B = rebalance_light();

		if(B.size()>1) {
			// rebalance
			double delta_zeta = rebalance(B);
			start_subround(total_zeta + delta_zeta);
			return;
		}
	}
	finish_round();
}



// initialize a new round
void coordinator::finish_round()
{
	// collect all data
	sketch newE(query.E.proj);
	for(auto p : proxy) {
		compressed_sketch csk = p.second->get_drift();
		newE += csk.sk;
		total_updates += csk.updates;
	}
	newE /= (double)k;

#if 1
	trace_round(newE);
#endif

	// new round
	query.update_estimate(newE);
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

		double zeta_total=0.0;
		double minzeta_total=0.0;
		double minzeta_min = INFINITY;
		valarray<size_t> round_updates((size_t)0, k);

		for(size_t i=0; i<k; i++) 
		{
			auto ni = node_ptr[i];
			zeta_total += ni->zeta;
			minzeta_total += ni->minzeta;
			minzeta_min = min(minzeta_min, ni->minzeta);
			round_updates[i] += ni->round_local_updates;
		}

		// check the value of the next E wrt this safe zone
		double norm_dE = norm_L2(newE);

		print("AGM Finish round: round updates=",round_updates.sum()," naive=",in_naive_mode, 
			"zeta_E=",query.zeta_E, "zeta_E'=", zeta_Enext, zeta_Enext/query.zeta_E,
			"||dE||=", norm_dE, norm_dE/query.zeta_E, 
			"zeta_total=", zeta_total/k, zeta_total/(k*query.zeta_E), 
			//"minzeta_min=", minzeta_total, minzeta_total/(k*query.zeta_E),
			//"minzeta_min/zeta_E=",minzeta_min/query.zeta_E,
			" QEst=", query.Qest,
   			" time=", (double)CTX.stream_count() / CTX.metadata().size() );

		// print the bit level
		print("               : c[",bit_level,"]=", elements_of(total_bitweight));
		// print elements
		print("               : S= ",elements_of(round_updates));

		// compute the bits w.r.t. the ball, i.e., for node i, 
		// compute  ||U[i]+E||/(zeta(E)/2)
		// BUT! Note that U[i] is the rebalanced thingy


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
	//query.update_estimate(dE/k);
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
	Qest_series("agm_qest", "%.10g", [&]() { return query.Qest;} )
{  
}

coordinator::~coordinator()
{
}

/*********************************************

	network

*********************************************/


agm::network::network(stream_id _sid, const projection& _proj, double _beta)
: 	star_network<network, coordinator, node>(CTX.metadata().source_ids()),
	sid(_sid), proj(_proj), beta(_beta) 
{
	set_name("AGM");
	
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

void agm::network::process_record()
{
	const dds_record& rec = CTX.stream_record();
	if(rec.sid==sid) 
		sites[rec.hid]->update_stream();		
}

void agm::network::process_init()
{
	// let the coordinator initialize the nodes
	CTX.timeseries.add(hub->Qest_series);
	hub->warmup();
	hub->start_round();
}


void agm::network::output_results()
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
