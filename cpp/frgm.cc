
#include <algorithm>
#include <boost/range/adaptors.hpp>
#include <boost/range/numeric.hpp>

#include "binc.hh"
#include "frgm.hh"

using namespace dds;
using namespace gm;
using namespace gm::frgm;

using binc::print;
using binc::elements_of;


/*********************************************
	node
*********************************************/

oneway node::reset(const safezone& newsz) 
{ 
	// reset the safezone object
	szone = newsz;

	// reset status
	is_rebalanced = false;
	is_flushed = false;

	// reset the drift vector
	U = 0.0;
	Uinc = 0.0;
	update_count = 0;
	zeta = minzeta = szone(Uinc);

	// reset for the first subround		
	reset_bitweight(zeta/2.0);

	// reset round state vector
	dS = 0.0;
	round_local_updates = 0;
}
	
float node::get_zeta() 
{
	return zeta;
}


oneway node::reset_bitweight(float Z)
{
	// flush if needed
	if(is_flushed) {
		is_flushed = false;

		U = 0.0;
		Uinc = 0.0;
		update_count = 0;
		zeta = 0.5*szone(Uinc);
	}

	minzeta = zeta_0 = zeta;
	zeta_quantum = Z;
	bitweight = 0;
}

compressed_state node::get_drift() 
{
	// switch on rebalanced status
	is_rebalanced = true;
	is_flushed = true;

	return compressed_state { U, update_count };
}



void node::update_stream() 
{
	assert(! is_flushed);

	assert(CTX.stream_record().hid == site_id());

	delta_vector delta = Q->delta_update(dS, CTX.stream_record());

	// oops, not an update
	if(delta.size()==0) return;

	update_count++;
	round_local_updates++;

	delta.apply_delta(U);

	if(is_rebalanced) {
		//
		// We are tracking (1/2) * zeta(E_0 + 2 DeltaS)
		//
		delta *= 2;
		delta.rebase_apply_delta(Uinc);
		zeta = 0.5 * szone(delta, Uinc);
	} else {
		//
		// We are tracking zeta(E_0 + DeltaS)
		//
		delta.rebase_apply_delta(Uinc);
		zeta = szone(delta, Uinc);
	}

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


node::node(network_t* net, source_id hid, continuous_query* _Q)
	: 	local_site(net, hid), Q(_Q),
		U(Q->state_vector_size()), 
		Uinc(Q->state_vector_size()), 
		update_count(0),
		dS(Q->state_vector_size()), round_local_updates(0),
		coord( this )
{ 
	coord <<= net->hub;
}


/*********************************************
	coordinator
*********************************************/

// initialize a new round
// and the first subround
void coordinator::start_round()
{
	// reset rebalancing
	B.clear();
	psi_Ebal = 0.0;
	DeltaEbal = 0.0;

	// update statistics
	round_sz_sent = 0;
	num_rounds++;
	num_subrounds++;

	// reset subround counter (for 1st subround)
	bitweight.assign(k, 0);
	total_bitweight.assign(k,0);

	bit_level = 1;
	bit_budget = k;

	// ship safe zone to nodes	
	for(auto n : node_ptr) {
		size_t nid = node_index[n];

		// send the right safezone to the node
		if(using_cost_model && !cmodel.d[nid]) 
			proxy[n].reset(safezone(radial_safe_zone));
		else {
			sz_sent++;
			proxy[n].reset(safezone(safe_zone));			
		}
	}
}	

//
// initialize a new subround (not the first)
// 
void coordinator::start_subround(double total_zeta)
{
	// reset counters
	num_subrounds++;
	bit_budget = k;
	bitweight.assign(k,0);

	// compute subround quantum
	double theta = (total_zeta + psi_Ebal)/(2.0*k);

	// reset nodes
	for(auto n : node_ptr) {
		proxy[n].reset_bitweight(theta);
	}	
}

//
// remote call on host violation
//
oneway coordinator::threshold_crossed(sender<node_t> ctx, int delta_bits)
{
	node_t* n = ctx.value;
	size_t nid = node_index[n];

	bitweight[nid] += delta_bits;
	total_bitweight[nid] += delta_bits;
	
	bit_budget -= delta_bits;

	if(bit_budget < 0) 
		finish_subround();
}

//
// 
//
void coordinator::finish_subround()
{
	// continue the aprroximation of zeta
	double total_zeta = 0.0;
	for(auto n : node_ptr) {
		total_zeta += proxy[n].get_zeta();
	}

	bit_level++;

	if( (total_zeta + psi_Ebal) < k * query->zeta_E * 0.01 ) 
		finish_subrounds(total_zeta);
	else 
		start_subround(total_zeta);
}



void coordinator::fetch_updates(node_t* n, Vec& S, size_t& upd)
{
	compressed_state cs = proxy[n].get_drift();
	S += cs.vec;
	upd += cs.updates;
	total_updates += cs.updates;
}


void coordinator::finish_subrounds(double total_zeta)
{
	/* 
		Try to rebalance all nodes.
	 */
	size_t nupdates = 0;
	for(auto n : node_ptr) {
		fetch_updates(n, DeltaEbal, nupdates);
		B.insert(n);
	}

	// cut off if very few updates
	if(nupdates <= 40*k) {
		//print(name(), "round=",num_rounds, " too few updates=",nupdates);
		finish_round();
		return;
	}

	assert(B.size()==k);

	double B2 = (B.size()/2.0);
	psi_Ebal =  B2 * query->compute_zeta(DeltaEbal / B2);

	// all nodes are balanced!
	total_zeta = query->zeta_E * k /2.0;

	//print(name(), "round=",num_rounds, "psi_Ebal=",psi_Ebal, "total_zeta=",total_zeta,
	//	"k*zeta_E=",k * query->zeta_E,"updates=",nupdates);

	if( (psi_Ebal + total_zeta) >= k * query->zeta_E * 0.1)
		start_subround(total_zeta);
	else
		finish_round();
}


// finish the round
void coordinator::finish_round()
{
	// check goodness
	//print(name(), ":at end of round",num_rounds,", zeta(newE)=", query->compute_zeta(DeltaEbal / (double)k), "zeta_E=",query->zeta_E);

	// collect all data
	finish_with_newE(DeltaEbal / (double)k);
}


void coordinator::finish_with_newE(const Vec& newE)
{
	if(using_cost_model) {
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


void coordinator::finish_rounds()
{
	coordinator::finish_round();	
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
		commcost += D;
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
	k(nw->hids.size()),
	DeltaEbal(0.0, Q->state_vector_size()),
	Qest_series(nw->name()+".qest", "%.10g", [&]() { return query->Qest;} ),
	num_rounds(0),
	num_subrounds(0),
	sz_sent(0),
	total_rbl_size(0),
	total_updates(0),
	cmodel(this)
{  
	set_name(nw->name()+":coord");
	safe_zone = query->safezone();
	radial_safe_zone = query->radial_safezone();	

	using_cost_model = radial_safe_zone!=nullptr && cfg().use_cost_model;
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
	double kzeta = (double)kk * zeta_E;
	assert(kzeta > 0.0);

	// normalize here, so that compute_model does not have to deal with zetas
	alpha /= kzeta;
	total_alpha /= kzeta;

	beta  /= kzeta;
	total_beta /= kzeta;

	assert(round_updates > 0.0);

	// we need to check for all zeros!
	if(total_alpha == total_beta) 
		total_beta += 1.0;

	alpha /= round_updates;
	beta  /= round_updates;
	gamma /= round_updates;
	total_alpha /= round_updates;
	total_beta /= round_updates;

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

	// We do not have enough data to have a reasonable chance at 
	// estimating the alphas
	if(round_updates <= 100.*(double)k) {
		tau_opt = 1.0/total_beta;
		max_gain = 0.0;
		return;
	}

	// Report on the accuracy of the previous estimate
	//if(max_gain<0) return;
	// print("Previous estimate: tau=", tau_opt," actual=", round_updates);
	// print("----");
	// print("Current tau bounds min=", 1./total_beta, "max=",1./total_alpha, 
	// 	"    total_alpha=",total_alpha,"total_beta=",total_beta, "kk=",kk);

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

	//print("invtau=",invtau,"total_beta=",total_beta);
	//assert(invtau == total_beta);

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

	//print("invtau=",invtau,"total_alpha=",total_alpha);

	assert(max_gain >= 0.0);
	assert(tau_opt >= 0.99/total_beta);  // give a 1% margin for roundoff errors
	assert(tau_opt <= 1.01/total_alpha); // give a 1% margin for roundoff errors

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


frgm::network::network(const string& _name, continuous_query* _Q)
	: 	gm_network_t(_name, _Q)
{
	this->set_protocol_name("FRGM");
}


gm::p_component_type< gm::frgm::network > gm::frgm::frgm_comptype("FRGM");


