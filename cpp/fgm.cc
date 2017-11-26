
#include <algorithm>
#include <boost/range/adaptors.hpp>

#include "results.hh"
#include "binc.hh"
#include "fgm.hh"

using namespace dds;
using namespace gm;
using namespace gm::fgm;

using binc::print;
using binc::elements_of;


/*********************************************
	node
*********************************************/

oneway node::reset(const safezone& newsz) 
{ 
	// reset the safezone object
	szone = newsz;

	// reset the drift vector
	U = 0.0;
	update_count = 0;
	zeta = minzeta = szone(U);

	// reset for the first subround		
	reset_bitweight(zeta/2.0);

	// reset round statistics
	dS = 0.0;
	round_local_updates = 0;
}

int node::set_safezone(const safezone& newsz) 
{
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
	
double node::get_zeta() 
{
	return zeta;
}

oneway node::reset_bitweight(double Z)
{
	minzeta = zeta_0 = zeta;
	zeta_quantum = Z;
	bitweight = 0;
}

compressed_state node::get_drift() 
{
	return compressed_state { U, update_count };
}

double node::set_drift(compressed_state newU) 
{
	U = newU.vec;
	update_count = newU.updates;
	double old_zeta = zeta;
	zeta = szone(U);
	return zeta-old_zeta;
}


void node::update_stream() 
{
	assert(CTX.stream_record().hid == site_id());

	delta_vector delta = Q->delta_update(dS, CTX.stream_record());

	// oops, not an update
	if(delta.size()==0) return;

	delta.apply_delta(U);

	update_count++;
	round_local_updates++;

	zeta = szone(delta, U);
	if(zeta<minzeta) minzeta = zeta;

	int bwnew = floor((zeta_0-minzeta)/zeta_quantum);
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

	round_sz_sent = 0;
	num_rounds++;
	num_subrounds++;
	
	//has_cheap_safezone.assign(k, in_naive_mode);
	has_cheap_safezone.assign(k, (radial_safe_zone!=nullptr) && cfg().use_cost_model);

	for(auto n : node_ptr) {
		// based on the above line this is unnecessary
		if(! has_cheap_safezone[node_index[n]]) {
			sz_sent++;
			proxy[n].reset(safezone(safe_zone));
		}
		else
			proxy[n].reset(safezone(radial_safe_zone));
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
oneway coordinator::threshold_crossed(sender<node_t> ctx, int delta_bits)
{
	node_t* n = ctx.value;
	size_t nid = node_index[n];

	// If the node has a cheap safe zone, send it the proper one (if applicable)
	if(has_cheap_safezone[nid] && cmodel.d[nid]) {
		// send the proper safezone to the node
		sz_sent++;
		round_sz_sent++;
		delta_bits += 
			proxy[n].set_safezone( safezone(safe_zone) );
		has_cheap_safezone[nid] = false;
	}

	bitweight[nid] += delta_bits;
	total_bitweight[nid] += delta_bits;
	
	bit_budget -= delta_bits;

	if(bit_budget < 0) 
		finish_subround();

}

void coordinator::finish_subround()
{
	// continue the aprroximation of zeta
	double total_zeta = 0.0;
	for(auto n : node_ptr) {
		total_zeta += proxy[n].get_zeta();
	}

	bit_level++;

	if(total_zeta < query->zeta_E * 0.05 ) 
		finish_subrounds(total_zeta);
	else 
		start_subround(total_zeta);
}


void coordinator::finish_subrounds(double total_zeta)
{
	/* 
		In this function, we may add code to try to rebalance.
	 */
	finish_round();
}



// initialize a new round
void coordinator::finish_round()
{
	// collect all data
	Vec newE(0.0, Q->state_vector_size());
	for(auto n : node_ptr) {
		compressed_state cs = proxy[n].get_drift();
		newE += cs.vec;
		total_updates += cs.updates;
	}
	newE /= (double)k;

	if(radial_safe_zone!=nullptr && cfg().use_cost_model) {
		cmodel.update_model();
		cmodel.compute_model();		
	}

#if 0
	trace_round(newE);
#endif

	// new round
	query->update_estimate(newE);
	start_round();
}




void coordinator::trace_round(const Vec& newE)
{
	Vec Enext = query->E + newE;
	double zeta_Enext = query->zeta(Enext);

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

	print("AGM Finish round: round updates=",round_updates_total,
		"zeta_E=",query->zeta_E, "zeta_E'=", zeta_Enext, zeta_Enext/query->zeta_E,
		"||dE||=", norm_dE, norm_dE/query->zeta_E, 
		"zeta_total=", zeta_total/k, zeta_total/(k*query->zeta_E),
	        "sz_sent=", round_sz_sent,
		//"minzeta_min=", minzeta_total, minzeta_total/(k*query->zeta_E),
		//"minzeta_min/zeta_E=",minzeta_min/query->zeta_E,
		" QEst=", query->Qest,
			" time=", (double)CTX.stream_count() / CTX.metadata().size() );

	// print the round comm gain
	const size_t D = query->E.size();
	long int commcost = 0;
	for(size_t i=0; i<k; i++) {
		commcost += min(round_updates[i], D);
		commcost += has_cheap_safezone[i] ? 0 : D;
	}
	print("Total comm cost=", commcost, "gain=", (long int)((long int)round_updates_total- (long int)commcost),
			" %=", (double)commcost / (double)round_updates_total );
	// print the bit level
	print("               : c[",bit_level,"]=", elements_of(total_bitweight));
	// print elements
	print("               : S= ",elements_of(round_updates));

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
	Vec dE(Q->state_vector_size());

	for(auto&& rec : CTX.warmup) 
		Q->update(dE, rec);

	query->update_estimate(dE/(double)k);
}


void coordinator::setup_connections()
{
	using boost::adaptors::map_values;
	proxy.add_sites(net()->sites);
	for(auto n : net()->sites) {
		node_index[n] = node_ptr.size();
		node_ptr.push_back(n);
	}
	assert(k == node_ptr.size());
}


coordinator::coordinator(network_t* nw, continuous_query* _Q)
: 	process(nw), proxy(this), 
	Q(_Q),
	query(Q->create_query_state()), 
	total_updates(0), 
	k(nw->hids.size()),
	Qest_series(nw->name()+".qest", "%.10g", [&]() { return query->Qest;} ),
	num_rounds(0),
	num_subrounds(0),
	sz_sent(0),
	total_rbl_size(0),
	cmodel(this)
{  
	set_name(nw->name()+":coord");
	safe_zone = query->safezone();
	radial_safe_zone = query->radial_safezone();
}

coordinator::~coordinator()
{
	if(radial_safe_zone) delete radial_safe_zone;
	delete safe_zone;
	delete query;
}


/*********************************************

	cost model

*********************************************/


cost_model::cost_model(coordinator* _coord)
	: coord(_coord), k(_coord->k)
{

	alpha.resize(k,0.0);
	beta.resize(k,0.0);
	gamma.resize(k,0.0);
	proper.resize(k);
	d.resize(k);

	// initialize with a "generous" plan
	d.assign(k, false);
	// designates an illegal optimization
	max_gain = -1.0; 
}


void cost_model::update_model()
{
	// size of state vector (actually, the difference to the cheap one)
	D = coord->safe_zone->zeta_size() - coord->radial_safe_zone->zeta_size();

	// Update site models. We do not charge communication, since
	// we have paid the cost for fetching U (we could have fetched dS instead!)
	auto& node = coord->node_ptr;

	// reset the total counters
	round_updates = 0.0;
	total_alpha = 0.0;
	total_beta = 0.0;
	
	// this is used to scale alpha and beta
	const double zeta_E = coord->query->zeta_E;
	double kzeta = (double)k * zeta_E;
	assert(kzeta > 0.0);

	// count the non-ignored sites
	size_t kk=0;

	// reset proper sites
	proper.assign(k,false);

	// collect the arrays
	for(size_t i=0;i<k;i++) {
		// get gamma
		gamma[i] = node[i]->round_local_updates;
		if(gamma[i]==0.0) continue;

		// Get beta
		beta[i] = zeta_E - coord->radial_safe_zone->compute_zeta(node[i]->dS);
		if(beta[i]==0.0) continue;
		assert(beta[i]>0.0);

		// Get alpha
		// Note: zeta(n)
		alpha[i] = zeta_E - coord->safe_zone->compute_zeta(node[i]->dS);


		if(alpha[i]<0.0) { beta[i]-=alpha[i]; alpha[i] = 0.0; }
		else if(alpha[i]>beta[i]) alpha[i] = beta[i];

		// normalize here, instead of the cost model
		alpha[i] /= kzeta;
		beta[i]  /= kzeta;

		assert(beta[i] >= alpha[i]);
		assert(alpha[i] >= 0.0);
		assert(gamma[i]>0.0);

		total_alpha += alpha[i];
		total_beta += beta[i];
		round_updates += gamma[i];

		proper[i] = true;
		kk++;
	}

	// No input maybe?
	if(kk==0) return;

	assert(round_updates > 0.0);

	// we need to check for all zeros!
	if(total_alpha == total_beta) 
		total_beta += 1.0;

	alpha /= round_updates;
	beta  /= round_updates;
	gamma /= round_updates;
	total_alpha /= round_updates;
	total_beta /= round_updates;

	// Report on the accuracy of the previous estimate
	//if(max_gain<0) return;
	//print("Previous estimate: tau=", tau_opt," actual=", round_updates);
}


void cost_model::compute_model()
{
	// initialize the plan pessimistically :-)
	d.assign(k, false);

	// Compute I, vector of non-trivial indices.
	// An index is trivial iff gamma[i]==0.
	vector<size_t> I;
	I.reserve(k);
	for(size_t i=0;i<k;i++) {
		if(proper[i])
			I.push_back(i);
	}
	size_t kk = I.size();

	// Nothing to do here! This configuration corresponds to a round that didn't
	// have any input, or it didn't have any movement in the cheap zones!
	if(kk==0) {
		max_gain = 0;
		tau_opt = 0;
		return;
	}

	// tau(d) = 1/(beta_total - dot(theta, alpha))
	Vec theta = beta-alpha;

	// sort indices in I in decreasing order of theta
	// breaking ties arbitrarily
	sort(I.begin(), I.end(), [&](size_t i, size_t j) { return theta[i]>theta[j]; });
	
	// Compute the prediction
	// Since theta is sorted descending, the optimal solution will be described
	// by having d[i] == find(I,i) < argmax_gain (i.e., i appears in the first 
	// armgax_gain items of I).
	//
	// Therefore, the goal is to compute the best value of argmax_gain
	size_t argmax_gain=0;

	// maximum gain so far...
	max_gain = -INFINITY;

	// The inverse of tau, initially equal to beta_total
	// compute total beta
	double invtau = 0.0;
	for(auto i : I) invtau += beta[i];
	assert(invtau > 0.0);

	//
	// Computing \f$\sum_{i=1}^k \min\{\gamma_i \tau, D\}\f$.
	//
	// During the optimization loop, time $\tau$ is increased as
	// consequtive solutions are found. Therefore, we simply
	// adopt a "merge" strategy: sorting gamma[i] (for i in I) in
	// decreasing order, we can sweep the sorted array (as \f$\tau\f$ grows).
	//

	// Make a sorted (decreasing) list of the gammas;
	vector<size_t> J = I;
	sort(J.begin(), J.end(), [&](size_t i, size_t j) { return gamma[i]>gamma[j]; });

	// \c idx_gamma is the sweep index (initially 0)
	size_t idx_gamma = 0;

	// Normalize the values of gamma to sum to 1, let sum_small_gamma be the
	// sum of all gamma[i] for i>=idx_gamma (initially, 1.0).
	double sum_small_gamma = 0.0;
	for(auto j : J) 
		sum_small_gamma += gamma[j];
	for(auto j : J) 
		gamma[j] /= sum_small_gamma;
	sum_small_gamma = 1.0;

	//
	//  Try solutions with sum(d[i])==n for each n=0..kk, and keep best.
	//  This is done by updating argmax_gain to contain the best n so far
	//
	for(size_t n=0; n<=kk; n++) {
		// compute tau (inverse)
		if(n>0) invtau -= theta[I[n-1]];

		// move the sweeping along reduce sum_small_gamma
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

		// total gain 
		double gain = 1./invtau - c_updates - n*D;

		// track maximum gain
		if(gain > max_gain) {
			tau_opt = 1/invtau;
			max_gain = gain;
			argmax_gain = n;
		}
	}

	assert(max_gain >= 0.0);

	for(size_t i=0; i<argmax_gain; i++)
		d[I[i]] = true;

	//print("Query plan for next round: ", elements_of(d));

	// finito
}




void cost_model::print_model()
{
	print("        Model alpha=", elements_of(alpha));
	print("        Model  beta=", elements_of(beta));
	print("        Model gamma=", elements_of(gamma));
}




/*********************************************

	network

*********************************************/


fgm::network::network(const string& _name, continuous_query* _Q)
	: 	star_network_t(CTX.metadata().source_ids()), Q(_Q)
{
	set_name(_name);
	this->set_protocol_name("AGMC");

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
	on(INIT, [&]() {
		CTX.timeseries.add(this->hub->Qest_series);
	});
	on(DONE, [&]() { 
		CTX.timeseries.remove(this->hub->Qest_series);
	});
}


network::~network()
{
	delete Q;
}


void fgm::network::process_record()
{
	const dds_record& rec = CTX.stream_record();
	this->source_site(rec.hid)->update_stream();		
}

void fgm::network::process_init()
{
	// let the coordinator initialize the nodes
	this->hub->warmup();
	this->hub->start_round();
}


void fgm::network::output_results()
{
	//network_comm_results.netname = "GM2";

	network_comm_results.fill_columns(this);
	network_comm_results.emit_row();

	network_host_traffic.output_results(this);
	network_interfaces.output_results(this);

	gm_comm_results.fill(this);
	gm_comm_results.emit_row();
}

gm::p_component_type< gm::fgm::network > gm::fgm::fgm_comptype("FGM");


