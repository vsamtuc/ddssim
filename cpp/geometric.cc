
#include "results.hh"
#include "binc.hh"
#include "geometric.hh"

using namespace dds;
using namespace dds::gm2;

using binc::print;

/*********************************************
	node
*********************************************/

void node::update_stream() 
{
	assert(CTX.stream_record().hid == site_id());

	U.update(CTX.stream_record().key, CTX.stream_record().upd);
	update_count++;

	zeta = szone(U);
	if(zeta<minzeta) minzeta = zeta;

	// if(fabs(zeta-last_zeta)>=szone.threshold) {
	int bwnew = floor((zeta_0-zeta)/zeta_quantum);
	int dbw = bwnew - bitweight;
	if(dbw>0) {
		bitweight = bwnew;
		coord.threshold_crossed(this, dbw);
	}
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
	updates.assign(k, 0);	
	msgs.assign(k, 0);

	bit_level = 1;
	bit_budget = k;

	if(query.zeta_E < sqrt(query.E.width())) {
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


// remote call on host violation
oneway coordinator::threshold_crossed(sender<node> ctx, int delta_bits)
{
	const int MAX_LEVEL = 4;
	node* n = ctx.value;

	assert(delta_bits>0);
	size_t nid = node_index[n];

	bitweight[nid] += delta_bits;
	msgs[nid]++;

	bit_budget -= delta_bits;

	if(bit_budget < 0) {

		if(bit_level < MAX_LEVEL) {
			// continue the aprroximation of zeta
			bit_level++;

			double total_zeta = 0.0;
			for(auto p : proxy) {
				total_zeta += p.second->get_zeta();				
			}

			if(total_zeta < query.zeta_E/(1<<MAX_LEVEL)) {
				// we are done!
				finish_round();				
			} else {
				bit_budget = k;
				bitweight.assign(k,0);
				for(auto p : proxy) {
					p.second->reset_bitweight(total_zeta/(2.0*k));
				}
			}


		} else {
			finish_round();
		}
	}

}


// initialize a new round
void coordinator::finish_round()
{
	// collect all data
	round_updates = 0;
	sketch newE(query.E.proj);
	for(auto p : proxy) {
		compressed_sketch csk = p.second->get_drift();
		newE += csk.sk;
		total_updates += csk.updates;
		round_updates += csk.updates;
	}
	newE /= (double)k;

#if 1

//#define VALIDATE_INVARIANTS
#ifdef VALIDATE_INVARIANTS

	//
	// validation
	//
	bool invariants_ok = true;

	// Check accuracy of incremental vs direct execution
	for(auto nptr : node_ptr) {
		// Check that the incremental and direct zetas match
		double node_zeta = nptr->zeta;
		double node_zeta_from_scratch = (has_naive[node_index[nptr]])? (query.zeta_E - norm_L2(nptr->U)) :
														query.safe_zone(query.E + nptr->U);
		if(fabs(node_zeta_from_scratch - node_zeta)>1.0E-6) {
			invariants_ok = false;
			print("*** PROBLEM: Too big a diversion in the incremental and direct zetas, node=", nptr->site_id());
		}
		if(has_naive[node_index[nptr]] != in_naive_mode) {
			invariants_ok = false;
			print("*** PROBLEM: node is marked naive, coord not in naive mode, node=", nptr->site_id());
		}
		if(has_naive[node_index[nptr]] != (nptr->szone.szone==nullptr)) {
			invariants_ok = false;
			print("*** PROBLEM: node marked naive != node.safezone.szone!=null, node=", nptr->site_id());
		}
	}		

	sketch Enext = query.E + newE;
	double zeta_Enext = query.safe_zone(Enext);

	double zeta_total=0.0;
	double minzeta_total=0.0;
	double minzeta_min = INFINITY;
	for(auto ni : node_ptr) {
		zeta_total += ni->zeta;
		minzeta_total += ni->minzeta;
		minzeta_min = min(minzeta_min, ni->minzeta);
	}

	if(zeta_Enext + 1E-6 < zeta_total/k ) {    // check that we didn't screw up!
		// recompute zetas from the source!
		print("*** PROBLEM: zeta_E(X)=",zeta_Enext," < (1/sum(X_i))=", zeta_total/k);
		invariants_ok=false;
	}
	if(!invariants_ok) {
		print("Finish round: updates=",total_updates," naive=",in_naive_mode, 
			"zeta_E=",query.zeta_E, "zeta_E'=", zeta_Enext, zeta_Enext/query.zeta_E, 
			"zeta_total=", zeta_total, zeta_total/(k*query.zeta_E), "minzeta_total=", minzeta_total, minzeta_total/(k*query.zeta_E),

			" time=", (double)CTX.stream_count() / CTX.metadata().size() );
		print("network dump: stream_count=",CTX.stream_count(),"cur_rec=",CTX.stream_record());
		for(auto nptr : node_ptr) {
			double node_zeta = nptr->zeta;
			double node_zeta_from_scratch = query.safe_zone(query.E + nptr->U);
			print("hid=",nptr->site_id(),"naive=",has_naive[node_index[nptr]],
				"node.zeta=",node_zeta,"from-scratch=",node_zeta_from_scratch, 
				"naive_z=", query.zeta_E - norm_L2(nptr->U),
				"error=",relative_error(node_zeta_from_scratch,node_zeta), 
				"minzeta=",nptr->minzeta," bitweight=",nptr->bitweight,"updates=",nptr->update_count);
		}

		assert(false);
	}  
#endif

	// report
	static int skipper = 0;
	skipper = (skipper+1)%50;

	if( !in_naive_mode || skipper==0 ) {
		// check the value of the next E wrt this safe zone

#ifndef VALIDATE_INVARIANTS
		sketch Enext = query.E + newE;
		double zeta_Enext = query.safe_zone(Enext);

		double zeta_total=0.0;
		double minzeta_total=0.0;
		double minzeta_min = INFINITY;
		for(auto ni : node_ptr) {
			zeta_total += ni->zeta;
			minzeta_total += ni->minzeta;
			minzeta_min = min(minzeta_min, ni->minzeta);
		}
#endif
		double norm_dE = norm_L2(newE);

		print("Finish round: round updates=",round_updates," naive=",in_naive_mode, 
			"zeta_E=",query.zeta_E, "zeta_E'=", zeta_Enext, zeta_Enext/query.zeta_E,
			"||dE||=", norm_dE, norm_dE/query.zeta_E, 
			"zeta_total=", zeta_total/k, zeta_total/(k*query.zeta_E), 
			//"minzeta_min=", minzeta_total, minzeta_total/(k*query.zeta_E),
			"minzeta_min/zeta_E=",minzeta_min/query.zeta_E,
			" time=", (double)CTX.stream_count() / CTX.metadata().size() );
		emit(RESULTS);
	}

#endif

	// new round
	query.update_estimate(newE);
	start_round();
}


void coordinator::warmup()
{
	sketch dE(net()->proj);

	for(auto&& rec : CTX.warmup) {
		if(rec.sid == net()->sid) 
			dE.update(rec.key, rec.upd);
	}
	query.update_estimate(dE/k);
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
	Qest_series("gm2_", "%.10g", [&]() { return k*k*query.Qest;} )
{  
	CTX.timeseries.add(Qest_series);
}


/*********************************************

	network

*********************************************/


gm2::network::network(stream_id _sid, const projection& _proj, double _beta)
: 	star_network<network, coordinator, node>(CTX.metadata().source_ids()),
	sid(_sid), proj(_proj), beta(_beta) 
{
	set_name("GM2");
	
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

void gm2::network::process_record()
{
	const dds_record& rec = CTX.stream_record();
	if(rec.sid==sid) 
		sites[rec.hid]->update_stream();		
}

void gm2::network::process_init()
{
	// let the coordinator initialize the nodes
	hub->warmup();
	hub->start_round();
}


void gm2::network::output_results()
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