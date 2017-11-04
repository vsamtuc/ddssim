
#include <algorithm>
#include <boost/range/adaptors.hpp>

#include "results.hh"
#include "binc.hh"
#include "agm.hh"

using namespace dds;
using namespace dds::agm;

using binc::print;
using binc::elements_of;

//#define TRACE_RUN

/*********************************************
	node
*********************************************/

void node::update_stream() 
{
	assert(CTX.stream_record().hid == site_id());

	U.update(CTX.stream_record().key, num_sites * (CTX.stream_record().upd));
	dS.update(CTX.stream_record().key, num_sites * (CTX.stream_record().upd));

	update_count++;
	round_local_updates++;

	zeta = szone(U, zeta_l, zeta_u);
	if(zeta<minzeta) minzeta = zeta;

	int bwnew = floor((zeta_0-zeta)/zeta_quantum);
	int dbw = bwnew - bitweight;
	if(dbw!=0) {
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

	round_sz_sent = 0;
	num_rounds++;
	num_subrounds++;
	
#if 0
	// heuristic rule to harden the system in times of high variability
	if(query.zeta_E < k*sqrt(query.E.width())) {
		if(!in_naive_mode)
			print("SWITCHING TO NAIVE MODE stream_count=",CTX.stream_count());
		in_naive_mode = true;
	} else {
		if(in_naive_mode)
			print("SWITCHING TO FULL MODE stream_count=",CTX.stream_count());
		in_naive_mode = false;
	}
#else
	in_naive_mode = false;
#endif 	

	//has_naive.assign(k, in_naive_mode);
	has_naive.assign(k, true);

	for(auto n : node_ptr) {
		// based on the above line this is unnecessary
		if(! has_naive[node_index[n]]) {
			sz_sent++;
			proxy[n].reset(safezone(&query.safe_zone, &query.E, total_updates, query.zeta_E));
		}
		else
			proxy[n].reset(safezone(query.zeta_E));
	}
}	


void coordinator::start_subround(double total_zeta)
{
	num_subrounds++;
	bit_budget = k;
	bitweight.assign(k,0);
	for(auto n : node_ptr) {
		proxy[n].reset_bitweight(total_zeta/(2.0*k));
	}	
}


// remote call on host violation
oneway coordinator::threshold_crossed(sender<node> ctx, int delta_bits)
{
	node* n = ctx.value;
	size_t nid = node_index[n];

	if(has_naive[nid] && md[nid]) {
		// send the proper safezone to the node
		sz_sent++;
		round_sz_sent++;
		delta_bits += 
			proxy[n].set_safezone(
				safezone(&query.safe_zone, &query.E, total_updates, query.zeta_E) );
		has_naive[nid] = false;
	}

	bitweight[nid] += delta_bits;
	total_bitweight[nid] += delta_bits;
	
	bit_budget -= delta_bits;

	if(bit_budget < 0) 
		finish_subround();

}

void coordinator::finish_subround()
{
	const int MAX_LEVEL = 300;

	if(bit_level < MAX_LEVEL) {
		// continue the aprroximation of zeta
		bit_level++;

		double total_zeta = 0.0;
		for(auto n : node_ptr) {
			total_zeta += proxy[n].get_zeta();
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

	total_rbl_size += B.size();	

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
		for(auto n : node_ptr) B.insert(& proxy[n]);

	return B;
}


vector<node_double> coordinator::compute_hvalue()
{
	vector<node_double> hvalue;
	hvalue.reserve(k);

	for(auto n : node_ptr) {
		double h = proxy[n].get_zeta_lu();
		hvalue.push_back(make_tuple(& proxy[n], h));
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
	for(auto n : node_ptr) {
		compressed_sketch csk = proxy[n].get_drift();
		newE += csk.sk;
		total_updates += csk.updates;
	}
	newE /= (double)k;

	// Update site models. We do not charge communication, since
	// we have paid the cost for fetching U (we could have fetched dS instead!)
	double round_updates = 0.0;
	for(size_t i=0;i<k;i++) {
		auto n = node_ptr[i];
		round_updates += n->round_local_updates;
	}
	assert(round_updates > 0.0);
	
	const double kzeta = k*query.zeta_E;

	for(size_t i=0;i<k;i++) {
		auto n = node_ptr[i];
		if(n->round_local_updates == 0)
			alpha[i]=beta[i]=gamma[i] = 0.0;
		else {
			gamma[i] = n->round_local_updates/round_updates;
			if(gamma[i]==0.0)
				alpha[i] = beta[i] = 0.0;
			else {
				beta[i] = norm_L2(n->dS)/round_updates;
				alpha[i] = (query.zeta_E - query.safe_zone(query.E + n->dS))/round_updates;
				// just a guess
				if(alpha[i]<0.0) alpha[i]=beta[i]/50.0;

				// normalize
				alpha[i] /= kzeta;
				beta[i] /= kzeta;
			}
		}
	}
	compute_model();


#ifdef TRACE_RUN
	trace_round(newE);
#endif

	// new round
	query.update_estimate(newE);
	start_round();
}



void coordinator::compute_model()
{
	// size of state vector
	const size_t D = query.E.size();

	// compute I, vector of non-trivial indices
	vector<size_t> I;
	I.reserve(k);
	for(size_t i=0;i<k;i++) {
		if(gamma[i]==0)
			md[i] = false;
		else
			I.push_back(i);
		assert(alpha[i]<=beta[i]);
	}
	size_t kk = I.size();

	Vec theta = beta-alpha;

	// sort indices in I in decreasing order of theta
	// breaking ties arbitrarily
	sort(I.begin(), I.end(), [&](size_t i, size_t j) { return theta[i]>theta[j]; });

	// compute total beta
	double beta_total = 0.0;
	for(auto i : I) beta_total += beta[i];
	assert(beta_total>0.0);
	
	// Make a sorted (decreasing) list of the gammas;
	vector<size_t> J = I;
	sort(J.begin(), J.end(), [&](size_t i, size_t j) { return gamma[i]>gamma[j]; });

	// compute the prediction
	size_t argmax_gain=0;
	double max_gain = -INFINITY;
	double invtau = beta_total;

	double sum_small_gamma = 0.0;
	for(auto j : J) 
		sum_small_gamma += gamma[j];
	for(auto j : J) 
		gamma[j] /= sum_small_gamma;
	sum_small_gamma = 1.0;

	size_t idx_gamma = 0;

	for(size_t n=0; n<=kk; n++) {
		// compute tau (inverse)
		if(n>0) invtau -= theta[I[n-1]];

		// reduce sum_small_gamma
		while(idx_gamma < J.size() && gamma[J[idx_gamma]] > D*invtau ) {
			sum_small_gamma -= gamma[J[idx_gamma]];
			idx_gamma++;
		}
		// precision troubles
		assert(sum_small_gamma >= -1E-6);
		if(sum_small_gamma < 0)
			sum_small_gamma = 0.0;

		// cost of shipping updates
		double c_updates = sum_small_gamma/invtau + D*idx_gamma;
#ifdef TRACE_RUN
		if(c_updates > 1/invtau + 1E-6) {
			print("Invariant error: c_updates=",c_updates, "invtau=",invtau,"1/invtau=", 1/invtau);
			print("n=",n,"sum_small_gamma=", sum_small_gamma,"theta=",theta[I[n-1]]);
			print("I=",elements_of(I));
			print_model();
			assert(false);
		}
#endif
		// total gain 
		double gain = 1./invtau - c_updates - n*D;

		// track maximum gain
		if(gain > max_gain) {
			max_gain = gain;
			argmax_gain = n;
		}
	}

	assert(max_gain >= 0.0);

	md.assign(k, false);
	for(size_t i=0; i<argmax_gain; i++)
		md[I[i]] = true;

	// finito
#ifdef TRACE_RUN
	print("        Model   tau=", 1/invtau, " gain=", max_gain, " sz_tosend=",argmax_gain, " D=",D);
	print("      md=",elements_of(md));
	//print_model();
#endif
}


void coordinator::print_model() 
{
	print("        Model alpha=", elements_of(alpha));
	print("        Model  beta=", elements_of(beta));
	print("        Model gamma=", elements_of(gamma));
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

		long int round_updates_total = round_updates.sum();

		// check the value of the next E wrt this safe zone
		double norm_dE = norm_L2(newE);

		print("AGM Finish round: round updates=",round_updates_total," naive=",in_naive_mode, 
			"zeta_E=",query.zeta_E, "zeta_E'=", zeta_Enext, zeta_Enext/query.zeta_E,
			"||dE||=", norm_dE, norm_dE/query.zeta_E, 
			"zeta_total=", zeta_total/k, zeta_total/(k*query.zeta_E),
		        "sz_sent=", round_sz_sent,
			//"minzeta_min=", minzeta_total, minzeta_total/(k*query.zeta_E),
			//"minzeta_min/zeta_E=",minzeta_min/query.zeta_E,
			" QEst=", query.Qest,
   			" time=", (double)CTX.stream_count() / CTX.metadata().size() );

		// print the round comm gain
		const size_t D = query.E.size();
		long int commcost = 0;
		for(size_t i=0; i<k; i++) {
			commcost += min(round_updates[i], D);
			commcost += has_naive[i] ? 0 : D;
		}
		print("Total comm cost=", commcost, "gain=", (long int)((long int)round_updates_total- (long int)commcost),
				" %=", (double)commcost / (double)round_updates_total );
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

void coordinator::print_state()
{
	printf("nid zeta.... c... zeta_0..\n");
	double zeta_t = 0.0;
	int c_t = 0;
	double zeta_0_t = 0.0;
	for(auto n : node_ptr) {
		auto nid = node_index[n];

		auto zeta = n->zeta;
		zeta_t += zeta;

		auto c = n->bitweight;
		c_t += c;

		auto zeta_0 = n->zeta_0;
		zeta_0_t += zeta_0;

		printf("%3lu %8g %4d %8g\n",nid,zeta,c,zeta_0);
	}
	printf("---------------------------\n");
	printf("SUM  %8g %4d %8g\n",zeta_t,c_t,zeta_0_t);

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
	using boost::adaptors::map_values;
	proxy.add_sites(net()->sites);
	for(auto n : net()->sites) {
		node_index[n] = node_ptr.size();
		node_ptr.push_back(n);
	}
	k = node_ptr.size();

	alpha.resize(k,0.0);
	beta.resize(k,0.0);
	gamma.resize(k,0.0);
	md.resize(k);
}


coordinator::coordinator(network* nw, const projection& proj, double beta)
: 	process(nw), proxy(this), 
	query(beta, proj), total_updates(0), 
	in_naive_mode(true), k(0),
	Qest_series(nw->name()+".qest", "%.10g", [&]() { return query.Qest;} ),
	
	num_rounds(0),
	num_subrounds(0),
	sz_sent(0),
	total_rbl_size(0)
{  
}

coordinator::~coordinator()
{
}

/*********************************************

	network

*********************************************/


agm::network::network(const string& _name, stream_id _sid, const projection& _proj, double _beta)
: 	star_network<network, coordinator, node>(CTX.metadata().source_ids()),
	sid(_sid), proj(_proj), beta(_beta) 
{
	set_name(_name);
	set_protocol_name("AGMC");
	
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
	on(INIT, [&]() {
		CTX.timeseries.add(hub->Qest_series);
	});
	on(DONE, [&]() { 
		CTX.timeseries.remove(hub->Qest_series);
	});
}




void agm::network::process_record()
{
	const dds_record& rec = CTX.stream_record();
	if(rec.sid==sid) 
		source_site(rec.hid)->update_stream();		
}

void agm::network::process_init()
{
	// let the coordinator initialize the nodes
	hub->warmup();
	hub->start_round();
}


void agm::network::output_results()
{
	//network_comm_results.netname = "GM2";

	network_comm_results.fill_columns(this);
	network_comm_results.emit_row();

	network_host_traffic.output_results(this);
	network_interfaces.output_results(this);

	gm_comm_results.fill(this);
	gm_comm_results.emit_row();
}
