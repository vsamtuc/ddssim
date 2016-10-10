#ifndef AMS_HPP
#define	AMS_HPP

#include <map>
#include "prng.hh"

// ams.h -- header file for Alon-Matias-Szegedy sketches
// using pairwise hash functions to speed up updates, Graham Cormode 2003

//#define SKDEPTH 7
//#define SKTHRESH 3
//#define SKMED 4

// define smaller sketches for debugging purposes (SKDEPTH = 4 * log(1/d))
// with SKDEPTH = 3, d = 0.178, so probabilistic confidence of sketches = 82.2%
//#define SKDEPTH 3
//#define SKTHRESH 1
//#define SKMED 2

using namespace std;

class AMS_hash_family
{
public:
	int depth;
	long long int *test[6];

	AMS_hash_family(int d);
	~AMS_hash_family();


	inline long hash31(int j, long long x) const {
		return ::hash31(test[0][j], test[1][j], x);
	}

	inline long fourwise(int j, long long x) const {
		return ::fourwise(test[2][j], test[3][j],
                test[4][j], test[5][j], x);
	}

	static map<int,AMS_hash_family*> cache;
	static inline AMS_hash_family* get_cached(int depth) {
		if(cache.find(depth)!=cache.end())
			return cache[depth];
		else {
			AMS_hash_family* ret = new AMS_hash_family(depth);
			cache[depth] = ret;
			return ret;
		}
	}
};

class AGMS
{
public:
    int depth;
    int buckets;
    int count;
    double *counts;
    const AMS_hash_family* hashf;


    AGMS(int buckets, int depth, const AMS_hash_family*);
    AGMS(int buckets, int depth);
    AGMS(const AGMS&);
    AGMS(AGMS&&);
    ~AGMS();

    inline long AMS_hash(int j, long long int item) const {
    	return hashf->hash31(j, item) % buckets;
    }
    inline long AMS_fourwise(int j, long long int item) const {
    	return hashf->fourwise(j, item);
    }

    inline double& AMS_hash_loc(int j, long long int item) {
    	return counts[j*buckets + AMS_hash(j,item)];
    }
    inline bool AMS_fourwise_positive(int j, long long item) {
    	return (AMS_fourwise(j,item) & 1) != 0;
    }

    void Update(unsigned long item, int freq);
    void PrintCounts();
    bool Compatible(AGMS *b);
    int Compare(AGMS *b);
    int Count(int item);
    double SketchNormSquare();
    double SketchNorm();
    double SmartSketchNorm(double previousNorm, unsigned int *elements, int *valuesBefore);
    void Zero();
    double F2Est();
    double InnerProd(AGMS * b);
    double InnerProdRow(AGMS * b, int row);
    void Scale(AGMS * dest, double s);
    void Scale(double s);
    AGMS operator+ (const AGMS& source);
    AGMS operator- (const AGMS& source);
    void operator= (const AGMS& source);
    void operator= (int number);
    AGMS operator/ (int divisor);
    AGMS operator* (double mult);
    int AddOn(AGMS *source);
    int Subtract(AGMS * source);
    AGMS *SmartSubtract(AGMS *dest, unsigned int *elements);
    AGMS *SmartAdd(AGMS *dest, unsigned int *elements);
    AGMS *SmartDivide(int divisor, unsigned int *elements);
    AGMS *SmartMultiply(int multiplicator, unsigned int *elements);
    int Size();

    // Additional operators
    void add_weighted(AGMS* x, double wx);
};



#endif
