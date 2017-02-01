#ifndef __ACCURATE_H__
#define __ACCURATE_H__

#include <iostream>
#include <set>
#include <map>
#include <vector>

#include "dds.hh"
#include "method.hh"
#include "mathlib.hh"


namespace dds {

using std::set;
using std::cout;
using std::endl;

class data_source_statistics : public reactive
{
	set<stream_id> sids;
	set<source_id> hids;

	distinct_histogram<local_stream_id> lshist;
	frequency_vector<stream_id> stream_size;

	size_t scount=0;
	timestamp ts=-1, te=-1;
public:

	data_source_statistics(); 

	void process(const dds_record& rec);
	void finish();

	void report(std::ostream& s);
};


using std::map;


template <qtype QType>
class exact_method : public reactive
{
public:
	typedef typed_query<QType> query_type;
protected:
	query_type Q;
	double curest = 0.0;
public:

	const query_type& query() const { return Q; }

	inline double current_estimate() const { return curest; }
};


class selfjoin_exact_method : public exact_method<qtype::SELFJOIN>
{
	const self_join Q;

	distinct_histogram<key_type> histogram;
public:
	selfjoin_exact_method(stream_id sid);

	void process_record(const dds_record& rec);
	void finish();
};


class twoway_join_exact_method : public exact_method<qtype::JOIN>
{
	const twoway_join Q;

	typedef distinct_histogram<key_type> histogram;
	histogram hist1;
	histogram hist2;
	double curest = 0.0;

	// helper
	void dojoin(histogram& h1, histogram& h2, const dds_record& rec);
public:
	twoway_join_exact_method(stream_id s1, stream_id s2);

	void process_record(const dds_record& rec);
	void finish();
};



} // end namespace dds

#endif