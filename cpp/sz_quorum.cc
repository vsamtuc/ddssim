#include "sz_quorum.hh"


/////////////////////////////////////////////////////////
//
//  quorum_safezone
//
/////////////////////////////////////////////////////////

using namespace gm;

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


double quorum_safezone::zeta_eikonal(const Vec& zX) 
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



double quorum_safezone::zeta_non_eikonal(const Vec& zX) 
{
	Vec zEzX = zetaE;
	zEzX *= zX[L];
	std::nth_element(begin(zEzX), begin(zEzX)+(L.size()-k), end(zEzX));
	return std::accumulate(begin(zEzX), begin(zEzX)+(L.size()-k+1), 0.0);
}

