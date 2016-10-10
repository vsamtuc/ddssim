/********************************************************************
AMS Sketches
G. Cormode 2003

This version: 2003-12

This work is licensed under the Creative Commons
Attribution-NonCommercial License. To view a copy of this license,
visit http://creativecommons.org/licenses/by-nc/1.0/ or send a letter
to Creative Commons, 559 Nathan Abbott Way, Stanford, California
94305, USA. 
 *********************************************************************/
#include <agms.hh>
#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <iostream>

#include "massdal.hh"

extern int verbatim;

map<int,AMS_hash_family*> AMS_hash_family::cache;


AMS_hash_family::AMS_hash_family(int dep) : depth(dep)
{
    prng_type *prng;
    prng = prng_Init(-6321371, 2);

    // create space for the hash functions
    // the test[] array contains random numbers which are later used
    // for the production of pairwise independent hash functions
    for (int i = 0; i < 6; i++)
        test[i] = new long long int[depth];

    // initialise the hash functions
    // prng_int() should return a random integer
    // uniformly distributed in the range 0..2^31
    for (int i = 0; i < depth; i++)
    {
        // the test[] array contains random numbers which are later used
        // for the production of pairwise independent hash functions
        for (int j = 0; j < 6; j++)
        {
            test[j][i] = (long long) prng_int(prng);
            if (test[j][i] < 0) test[j][i] = -test[j][i];
            //printf("test[%d][%d]: %d\n", j, i, result->test[j][i]);
        }
    }

    prng_Destroy(prng);
}


AMS_hash_family::~AMS_hash_family()
{
    // destroy the data structure
    for (int i = 0; i < 6; i++)
        delete[] test[i];
}



AGMS::AGMS(int buckets, int depth, const AMS_hash_family* h)
	: hashf(h)
{
    this->depth = depth;
    this->buckets = buckets;
    count = 0;

    counts = new double[buckets*depth];
}

AGMS::AGMS(int buckets, int depth)
{
    this->depth = depth;
    this->buckets = buckets;
    count = 0;
    hashf = AMS_hash_family::get_cached(depth);
    counts = new double[buckets*depth];
}

AGMS::AGMS(const AGMS& x)
{
	depth = x.depth;
	buckets = x.buckets;
	count = x.count;
	hashf = x.hashf;
    counts = new double[buckets*depth];
    copy(x.counts, x.counts+depth*buckets, counts);
}

AGMS::AGMS(AGMS&& x)
{
    depth = x.depth;
    buckets = x.buckets;
    count = x.count;
    hashf = x.hashf;
    counts = x.counts;
    x.counts = nullptr; x.depth=0; x.buckets=0;
}


AGMS::~AGMS()
{
    if(counts)
        delete[] counts;
}

void AGMS::PrintCounts()
{
    int offset = 0;    
    for (int j = 0; j < depth; j++)
    {
        for (int i = 0; i < buckets; i++)
        {
            if (counts[offset + i] != 0)
                printf("[%d]: %5.2f, ", offset + i, counts[offset + i]);
        }
        offset += buckets;
    }
    printf("\n");
}

void AGMS::Update(unsigned long item, int freq)
{
    // update the sketch
    // hash to one bucket in each row
    // then multiply by {+1, -1} chosen at random

    unsigned int hash;
    int mult, offset;
    count += freq;
    offset = 0;
    
    
    for (int j = 0; j < depth; j++)
    {
    	hash = AMS_hash(j, item);
        mult = AMS_fourwise(j, item);

        if ((mult & 1) == 1)
        {
            counts[offset + hash] += freq;
        }
        else
        {
            counts[offset + hash] -= freq;
        }

        
        offset += buckets;
    }
    
}



bool AGMS::Compatible(AGMS *b)
{
    // test whether two sketches have the same parameters
    // if so, then they can be added, subtracted, etc.
	return b &&
			buckets == b->buckets &&
			depth == b->depth &&
			hashf == b->hashf;
}

int AGMS::Compare(AGMS *b)
{
    int offset = 0;
    //int mismatch = 0;
    for (int j = 0; j < depth; j++)
    {
        for (int i = 0; i < buckets; i++)
        {
            if (counts[offset + i] != b->counts[offset + i])
            {
                //printf("sketch mismatch at point [%d].", offset + i);
                //mismatch = 1;
            	return 1;
            }
        }
        offset += buckets;
    }
    
    //return mismatch;
    return 0;
}

int AGMS::Count(int item)
{
    // compute the estimated count of item
    int offset = 0;
    unsigned int hash;
    int mult;
    int i;
    int estimates[depth];

    for (i = 0; i < depth; i++)
    {
        hash = AMS_hash(i, item);
        mult = AMS_fourwise(i, item);

        if ((mult & 1) == 1)
            estimates[i] = counts[offset + hash];
        else
            estimates[i] = -counts[offset + hash];
        offset += buckets;
    }

    if (depth == 1) i = estimates[0];
    else if (depth == 2) i = (estimates[0] + estimates[1]) / 2;
    else
        i = order_select(depth / 2, depth, estimates);
    return (i);
}

void AGMS::Zero()
{
    // set the sketch to zero
    int index = 0;

    for (int i = 1; i <= depth; i++)
        for (int j = 0; j < buckets; j++)
            counts[index++] = 0;
}

double AGMS::F2Est()
{
    // estimate the F2 moment of the vector (sum of squares)
    double result;
    double estimates[depth];

    int index = 0;
    for (int i = 0; i < depth; i++)
    {
        double sumOfSquares = 0.0;
        for (int j = 0; j < buckets; j++)
        {
            sumOfSquares += (counts[index] * counts[index]);
            index++;
        }
        estimates[i] = sumOfSquares;
    }
    if (depth == 1) result = estimates[0];
    else if (depth == 2) result = (estimates[0] + estimates[1]) / 2.0;
    else {
    	result = order_select(depth/2, depth, estimates);
    }
    return (result);
}

double AGMS::InnerProd(AGMS * b)
{
    double result, z;
    double estimates[depth];

    // estimate the innerproduct of two vectors using their sketches.
    assert(Compatible(b));

    int index = 0;
    for (int i = 0; i < depth; i++)
    {
        z = 0;
        for (int j = 0; j < buckets; j++)
        {
            z +=  counts[index] * b->counts[index];
            index++;
        }
        estimates[i] = z;
    }
    if (depth == 1) result = estimates[0];
    else if (depth == 2) result = (estimates[0] + estimates[1]) / 2;
    else
        result = order_select(depth / 2, depth, estimates);
    return (result);
}

double AGMS::InnerProdRow(AGMS *b, int row)
{
    // estimate the innerproduct of two vectors using their sketches.
    // picking out one row only

    //if (AMS_Compatible(a,b)==0) return 0;
    double z = 0;
    int index = buckets*row;

    // jump to the start of the desired row
    for (int j = 0; j < buckets; j++)
    {
        z += ((double) counts[index]* (double) b->counts[index]);
        index++;
    }
    return (z);
}

void AGMS::Scale(AGMS * dest, double s)
{
    // scale one sketch by a scalar
    int r = 0;
    dest->count *= s;
    for (int i = 0; i < dest->depth; i++)
        for (int j = 0; j < dest->buckets; j++)
        {
            dest->counts[r] *= s;
            r++;
        }
}

void AGMS::Scale(double s)
{
    // scale one sketch by a scalar
    int r = 0;
    for (int i = 0; i < this->depth; i++)
        for (int j = 0; j < this->buckets; j++)
        {
            this->counts[r] *= s;
            r++;
        }
}

AGMS AGMS::operator+ (const AGMS& source)
{
    AGMS result(source.buckets, source.depth,hashf);
    int r = 0;
    result.Zero();
    for (int i = 0; i < source.depth; i++)
    {
        for (int j = 0; j < source.buckets; j++)
        {
            result.counts[r] = this->counts[r] + source.counts[r];
            r++;
        }
    }

    return result;
}

AGMS AGMS::operator- (const AGMS& source)
{
    // subtract one sketch from another
    AGMS result(source.buckets, source.depth,hashf);
    int r = 0;

    for (int i = 0; i < source.depth; i++)
    {
        for (int j = 0; j < source.buckets; j++)
        {
            result.counts[r] = this->counts[r] - source.counts[r];
            r++;
        }
    }

    return result;
}

void AGMS::operator= (const AGMS& source)
{
    int r = 0;

    for (int i = 0; i < source.depth; i++)
        for (int j = 0; j < source.buckets; j++)
        {
            this->counts[r] = source.counts[r];
            r++;
        }
}

void AGMS::operator= (int number)
{
    int r = 0;

    for (int i = 0; i < this->depth; i++)
        for (int j = 0; j < this->buckets; j++)
        {
            this->counts[r] = number;
            r++;
        }
}

double AGMS::SketchNormSquare()
{
    double norm = 0;
    int r = 0;

    for (int i = 0; i < this->depth; i++)
        for (int j = 0; j < this->buckets; j++)
        {
            norm += counts[r] * counts[r];
            r++;
        }

    return norm;
}

double AGMS::SketchNorm()
{
	return sqrt(SketchNormSquare());
}

double AGMS::SmartSketchNorm(double previousNorm, unsigned int *elements, int *valuesBefore)
{
    double norm = previousNorm;
    
    for (int i = 0; i < this->depth; i++)
    {
        previousNorm += (this->counts[elements[i]] * this->counts[elements[i]]) -
                (valuesBefore[i] * valuesBefore[i]);
    }

    return sqrt(norm);
}

AGMS AGMS::operator/ (int divisor)
{
    AGMS result(this->buckets, this->depth, hashf);
    int r = 0;

    for (int i = 0; i < this->depth; i++)
        for (int j = 0; j < this->buckets; j++)
        {
            result.counts[r] = this->counts[r] / divisor;
            r++;
        }

    return result;
}

AGMS AGMS::operator* (double mult)
{
    AGMS result(this->buckets, this->depth, hashf);
    int r = 0;

    for (int i = 0; i < this->depth; i++)
        for (int j = 0; j < this->buckets; j++)
        {
            result.counts[r] = this->counts[r] * mult;
            r++;
        }

    return result;
}


int AGMS::AddOn(AGMS *source)
{
    // add one sketch to another
    int r = 0;

    assert(Compatible(source));

    for (int i = 0; i < source->depth; i++)
        for (int j = 0; j < source->buckets; j++)
        {
            counts[r] += source->counts[r];
            r++;
        }

    return 1;
}

AGMS *AGMS::SmartAdd(AGMS* dest, unsigned int* elements)
{
    for (int i = 0; i < this->depth; i++)
    {
        this->counts[elements[i]] = this->counts[elements[i]] + dest->counts[elements[i]];
    }
    
    return this;
}

AGMS *AGMS::SmartSubtract(AGMS* dest, unsigned int* elements)
{
    //AMS_type result(dest->buckets, dest->depth, hashf);
    
    for (int i = 0; i < this->depth; i++)
    {
        this->counts[elements[i]] = this->counts[elements[i]] - dest->counts[elements[i]];
    }
    
    return this;
}

AGMS *AGMS::SmartDivide(int divisor, unsigned int *elements)
{
    for (int i = 0; i < this->depth; i++)
    {
        this->counts[elements[i]] = this->counts[elements[i]] / divisor;
    }
    
    return this;
}

AGMS *AGMS::SmartMultiply(int multiplicator, unsigned int *elements)
{
    for (int i = 0; i < this->depth; i++)
    {
        this->counts[elements[i]] = this->counts[elements[i]] * multiplicator;
    }
    
    return this;
}

int AGMS::Subtract(AGMS * source)
{
    // subtract one sketch from another
    int r = 0;

    assert(Compatible(source));
    for (int i = 0; i < source->depth; i++)
        for (int j = 0; j < source->buckets; j++)
        {
            this->counts[r] -= source->counts[r];
            r++;
        }

    return 1;
}

int AGMS::Size()
{
    // return the space used in bytes of the sketch
    int size = (sizeof (int *))+(buckets * depth) * sizeof (int) +
            depth * 6 * sizeof (long long) + sizeof (AGMS);

    return size;
}

void AGMS::add_weighted(AGMS* x, double wx)
{
    // add one sketch to another
    int p = 0;

    for (int i = 0; i < depth; i++)
        for (int j = 0; j < buckets; j++)
        {
        	counts[p] += wx * (x->counts[p] - counts[p]);
            p++;
        }
}

