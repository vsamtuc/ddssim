
#include "safezone.hh"


using namespace dds;


/////////////////////////////////////////////////////////
//
//  quorum_safezone
//
/////////////////////////////////////////////////////////

quorum_safezone::quorum_safezone() 
: n(0) { }

quorum_safezone::quorum_safezone(const Vec& zE, size_t _k) 
{
	prepare(zE, _k);
}

void quorum_safezone::prepare(const Vec& zE, size_t _k) 
{
	n = zE.size();
	k = _k;

	// count legal inputs
	size_t Legal[n];
	size_t pos = 0;
	for(size_t i=0; i<n; i++)
		if(zE[i]>0) Legal[pos++] = i;

	// create the index and the other matrices
	Index Ltmp(Legal, pos);
	Ltmp.swap(L);
	zetaE.resize(L.size());
	zetaE = zE[L];

	assert(1<=k && k<=n);
	if(L.size()<k)
		throw std::length_error("The reference vector is non-admissible");
}


/*
	Helper to compute sum(zEzX[I])/sqrt(sum(zEzE[I]^2))
 */
inline static double zeta_I(size_t *I, size_t m, const Vec& zEzX, const Vec& zEzE)
{
	double num = 0.0;
	double denom = 0.0;
	for(size_t i=0; i<m; i++) {
		num += zEzX[I[i]];
		denom += zEzE[I[i]];
	}
	return num/sqrt(denom);
}

/*
	Helper to iterate over all strictly increasing m-long
	sequences over [0:l)
  */
inline static bool next_I(size_t *I, size_t m, size_t l) 
{
	for(size_t i=1; i<=m; i++) {
		if(I[m-i] < l-i) {
			I[m-i]++;
			for(size_t j=1;j<i;j++) 
				I[m-i+j] = I[m-i]+j;
			return true;
		}
	}
	return false;
}


double quorum_safezone::operator()(const Vec& zX) 
{
	// precompute  zeta_i(E)*zeta_i(X) for all i\in L
	Vec zEzX = zetaE;
	zEzX *= zX[L];

	// precompute zeta_i(E)^2
	Vec zE2 = zetaE*zetaE;

	size_t l = L.size();
	size_t m = L.size()-k+1;

	//
	// Each clause corresponds to an m-size subset I of [0:l).
	// The goal is to find the subset I where
	//   (\sum_i\in I  zXzE[i])  /  sqrt(\sum_i zE2) = zeta_I(I)
	// In lieu of a smart heuristic, this is now done exhaustively.
	//

	size_t I[m]; // holds the indices to the current m-set
	std::iota(I, I+m, 0);  // initialized to (0,...,0)

	// iteration is done with the help of next_I, which increments I

	double zinf = zeta_I(I, m, zEzX, zE2);
	while(next_I(I, m, l)) {
		double zI = zeta_I(I, m, zEzX, zE2);
		zinf = std::min(zI, zinf);
	}

	return zinf;
}



double quorum_safezone_fast::operator()(const Vec& zX) 
{
	Vec zEzX = zetaE;
	zEzX *= zX[L];
	std::nth_element(begin(zEzX), begin(zEzX)+(L.size()-k), end(zEzX));
	return std::accumulate(begin(zEzX), begin(zEzX)+(L.size()-k+1), 0.0);
}


/////////////////////////////////////////////////////////
//
//  selfjoin_agms_safezone_upper_bound
//  selfjoin_agms_safezone_lower_bound
//  selfjoin_agms_safezone
//
/////////////////////////////////////////////////////////


selfjoin_agms_safezone_upper_bound::selfjoin_agms_safezone_upper_bound(const sketch& E, double T)
: 	sqrt_T(sqrt(T)), Median()
{ 
	Vec dest = sqrt(dot_estvec(E));
	Median.prepare(sqrt_T - dest , (E.depth()+1)/2) ;
}


double selfjoin_agms_safezone_upper_bound::operator()(const sketch& X) 
{
	Vec z = sqrt_T - sqrt(dot_estvec(X));
	return Median(z);
}

double selfjoin_agms_safezone_upper_bound::with_inc(incremental_state& incstate, const sketch& X) 
{
	incstate = dot_estvec(X);
	Vec z = sqrt_T - sqrt(incstate);
	return Median(z);
}


double selfjoin_agms_safezone_upper_bound::inc(incremental_state& incstate, const delta_vector& DX) 
{
	Vec z = sqrt_T - sqrt(dot_estvec_inc(incstate, DX));
	return Median(z);
}


selfjoin_agms_safezone_lower_bound::selfjoin_agms_safezone_lower_bound(const sketch& E, double T)
: 	Ehat(E), sqrt_T(sqrt(T))
{ 
	assert(T>0.0);   // T must be positive
	Vec dest = sqrt(dot_estvec(E));
	Median.prepare( dest - sqrt_T, (E.depth()+1)/2);

	//
	// Normalize E: divide each row  E_i by ||E_i||
	//
	size_t L = E.width();
	for(size_t d=0; d<E.depth(); d++) {
		if(dest[d]>0.0)  {
			// Note: this is an aliased assignment, but should
			// be ok, because it is pointwise aliased!
			Vec tmp = Ehat[slice(d*L,L,1)];
			Ehat[slice(d*L,L,1)] = tmp/dest[d];
		}
		// else, if dest[d]==0, the Ehat[slice(d)] == 0! leave it
	}
}

double selfjoin_agms_safezone_lower_bound::operator()(const sketch& X) 
{
	Vec z = dot_estvec(X,Ehat) - sqrt_T ;
	return Median(z);
}

double selfjoin_agms_safezone_lower_bound::with_inc(incremental_state& incstate, const sketch& X)
{
	incstate = dot_estvec(X,Ehat);
	Vec z = incstate - sqrt_T;
	return Median(z);
}


double selfjoin_agms_safezone_lower_bound::inc(incremental_state& incstate, const delta_vector& DX)
{
	Vec z = dot_estvec_inc(incstate, DX, Ehat) - sqrt_T ;
	return Median(z);
}


selfjoin_agms_safezone::selfjoin_agms_safezone(const sketch& E, double Tlow, double Thigh)
: 	lower_bound(E, Tlow),
	upper_bound(E, Thigh)
{
	assert(Tlow < Thigh);
}

selfjoin_agms_safezone::selfjoin_agms_safezone(selfjoin_query& q) 
:  selfjoin_agms_safezone(q.E, q.Tlow, q.Thigh)
{ }


// from-scratch computation
double selfjoin_agms_safezone::operator()(const sketch& X) 
{
	return min(lower_bound(X), upper_bound(X));
}


double selfjoin_agms_safezone::with_inc(incremental_state& incstate, const sketch& X) {
	return min(lower_bound.with_inc(incstate.lower, X), 
		upper_bound.with_inc(incstate.upper, X));
}

double selfjoin_agms_safezone::inc(incremental_state& incstate, const delta_vector& DX) {
	return min(lower_bound.inc(incstate.lower, DX), 
		upper_bound.inc(incstate.upper, DX));
}



