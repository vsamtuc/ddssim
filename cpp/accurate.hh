#ifndef __ACCURATE_H__
#define __ACCURATE_H__

#include <iostream>
#include <set>
#include <map>
#include <vector>
#include <string>

#include "dds.hh"
#include "method.hh"
#include "mathlib.hh"
#include "output.hh"
#include "agms.hh"

namespace dds {

using std::set;
using std::cout;
using std::endl;

using namespace std::string_literals;

class data_source_statistics : public reactive
{
	set<stream_id> sids;
	set<source_id> hids;

	distinct_histogram<local_stream_id> lshist;
	frequency_vector<stream_id> stream_size;

	size_t scount=0;
	timestamp ts=-1, te=-1;

	void process(const dds_record& rec);
	void finish();
	void report(std::ostream& s);

public:
	data_source_statistics(); 
};


using std::map;

/*************************************
 *
 *  Methods based on histgrams
 *
 *************************************/

template <qtype QType>
class exact_method : public reactive
{
public:
	typedef typed_query<QType> query_type;
protected:
	query_type Q;
	double curest = 0.0;

	column<double> series;
public:
	exact_method(query_type _Q) 
	: Q(_Q), series("HIST:"s+repr(Q), string("%.0f").c_str())
	{
		CTX.timeseries.add(series);
	}

	const query_type& query() const { return Q; }

	inline double current_estimate() const { return curest; }
};


class selfjoin_exact_method : public exact_method<qtype::SELFJOIN>
{
	distinct_histogram<key_type> histogram;

	void process_record(const dds_record& rec);
	void finish();
public:
	selfjoin_exact_method(stream_id sid);
};


class twoway_join_exact_method : public exact_method<qtype::JOIN>
{
	typedef distinct_histogram<key_type> histogram;
	histogram hist1;
	histogram hist2;

	// helper
	void dojoin(histogram& h1, histogram& h2, const dds_record& rec);
	// callbacks
	void process_record(const dds_record& rec);
	void finish();
public:
	twoway_join_exact_method(stream_id s1, stream_id s2);
};


/*************************************
 *
 *  Methods based on AGMS sketches
 *
 *************************************/




template <qtype QType>
class agms_method : public reactive
{
public:
	typedef typed_query<QType> query_type;
	typedef typename agms::index_type index_type;
	typedef typename agms::depth_type depth_type;
protected:
	query_type Q;
	double curest = 0.0;

	column<double> series;

public:
	static string make_name(query_type q, 
		depth_type D, index_type L) 
	{
		using std::ostringstream;
		ostringstream s;
		s << "AGMS<"<<D<<"x"<<L<<">:" << repr(q);
		return s.str();
	}


	agms_method(query_type _Q, agms::depth_type _D, agms::index_type _L) 
	: Q(_Q), series(make_name(_Q,_D,_L), "%.0f")
	{
		CTX.timeseries.add(series);
	}

	const query_type& query() const { return Q; }

	inline double current_estimate() const { return curest; }
};


class selfjoin_agms_method : public agms_method<qtype::SELFJOIN>
{
	agms::sketch sk;

	void process_record(const dds_record& rec);
	void finish();
public:
	selfjoin_agms_method(stream_id sid, agms::depth_type D, agms::index_type L);
};


class twoway_join_agms_method : public agms_method<qtype::JOIN>
{
	agms::sketch sk1, sk2;

	// callbacks
	void process_record(const dds_record& rec);
	void finish();
public:
	twoway_join_agms_method(stream_id s1, stream_id s2, agms::depth_type D, agms::index_type L);
};




} // end namespace dds

#endif