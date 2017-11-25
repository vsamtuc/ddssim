
#include "sz_quorum.hh"
#include <vector>

/////////////////////////////////////////////////////////
//
//  quorum_safezone
//
/////////////////////////////////////////////////////////

using namespace gm;
using std::vector;

quorum_safezone::quorum_safezone() 
{ }

quorum_safezone::quorum_safezone(const Vec& zE, size_t _k, bool _eik) 
{
	prepare(zE, _k);
	set_eikonal(_eik);
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
	if(L.size()<k) {
		throw std::length_error(binc::sprint("The reference vector is non-admissible:",zE));
	}
}


// Helper recursion for caching clause denominators
static void fill_denom(size_t m, size_t l, size_t b, const Vec& zE2, double SzE2, double*& D)
{
	if(m==0) 
		*D++ = sqrt(SzE2);
	else {
		const size_t c = l-m+1;
		for(size_t i=b; i< c; i++) 
			fill_denom(m-1, l, i+1, zE2, SzE2 + zE2[i], D);
	}
}

void quorum_safezone::prepare_z_cache()
{
	// Cache the denominators if l <= 19 (the array size is 
	// up to (19 choose 10)==92378 elements in size!)
	if(L.size()<=cached_bound) {
		// Compute all $\sqrt{\sum_{i\in I} \zeta(E_i)^2}$ for $I\in \binom{L}{|L|-k+1}$.
	
		Vec zE2 = zetaE * zetaE;  // Compute all squares

		// compute C = (l choose m)
		size_t l = zE2.size();
		size_t m = l-k+1;
		size_t C = 1;
		for(size_t i=1; i<= l-m; i++)
			C = (C*(m+i))/i;

		// prepare the array
		z_cached.resize(C);
		double* D = begin(z_cached);  // NOTE: We are assuming that begin(Vec) is double* !!!

		// Recursion
		fill_denom(m, l, 0, zE2, 0.0, D);
	}
	else {
		// For larger l, just cache $\zeta(E_i)^2$ (saving us some multiplications)
		z_cached = zetaE*zetaE;
	}	
}


// This recursion is used to compute zinf, when the cached array is just $\zeta(E_i)^2$.
// The recursion performs 2C additions, C divisions, C square roots and C comparisons,
// where C = (l choose m)
static double find_min(size_t m, size_t l, size_t b, const Vec& zEzX, const Vec& zE2, double SzEzX, double SzE2)
{
	if(m==0) return SzEzX/sqrt(SzE2);

	double zinf = find_min(m-1, l, b+1, zEzX, zE2, SzEzX + zEzX[b], SzE2 + zE2[b]);

	const size_t c = l-m+1;
	for(size_t i=b+1; i< c; i++) {
		double zi = find_min(m-1, l, i+1, zEzX, zE2, SzEzX + zEzX[i], SzE2 + zE2[i]);
		zinf = std::min(zinf, zi);
	}

	return zinf;
}

// This recursion is used to compute zinf, when the cached array is all clause denominators.
// The recursion performs C additions, C divisions C comparisons,
// where C = (l choose m). 
// In particular, no sqrt() are performed!
static double find_min_cached(size_t m, size_t l, size_t b, const Vec& zEzX, double SzEzX, double*& D)
{
	if(m==0) { 
		return SzEzX/(*D++);
	}

	double zinf = find_min_cached(m-1, l, b+1, zEzX, SzEzX + zEzX[b], D);

	const size_t c = l-m+1;
	for(size_t i=b+1; i< c; i++) {
		double zi = find_min_cached(m-1, l, i+1, zEzX, SzEzX + zEzX[i], D);
		zinf = std::min(zinf, zi);
	}

	return zinf;
}

double quorum_safezone::zeta_eikonal(const Vec& zX) 
{
	// precompute  zeta_i(E)*zeta_i(X) for all i\in L
	Vec zEzX = zetaE;
	zEzX *= zX[L];

	if(z_cached.size()==0)
		prepare_z_cache();

	const size_t l = L.size();
	const size_t m = l-k+1;

	//
	// Select the appropriate algorithm for the precomputed values
	// in z_cached.
	//
	double zinf;

	if(L.size()<=cached_bound) {
		// precomputed denominators
		double* D = begin(z_cached);  // Assuming begin(z_cached) is a double* !!!
		zinf = find_min_cached(m, l, 0, zEzX, 0.0, D);
		assert(D==end(z_cached));
	} else {
		// precomputed zeta_i(E)^2
		zinf = find_min(m, l, 0, zEzX, z_cached, 0.0, 0.0);
	}

	return zinf;
}



double quorum_safezone::zeta_non_eikonal(const Vec& zX) 
{
	Vec zEzX = zetaE;
	zEzX *= zX[L];
	std::nth_element(begin(zEzX), begin(zEzX)+(L.size()-k), end(zEzX));
	return std::accumulate(begin(zEzX), begin(zEzX)+(L.size()-k+1), 0.0);
}

