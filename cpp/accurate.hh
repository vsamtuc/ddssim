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
#include "results.hh"
#include "agms.hh"

namespace dds {

using std::set;
using std::map;
using std::cout;
using std::endl;

using namespace std::string_literals;

class data_source_statistics : public reactive
{
	set<stream_id> sids;
	set<source_id> hids;

	// count total size of local streams
	distinct_histogram<local_stream_id> lshist;

	// count each stream size
	frequency_vector<stream_id> stream_size;

	// timeseries for 'active' records per local stream
	typedef column<int> col_t;
	map<local_stream_id, col_t*> lssize;

	// timeseries for 'active' records per stream
	map<stream_id, col_t*> ssize;

	// timeseries for 'active' records per source
	map<source_id, col_t*> hsize;

	size_t scount=0;
	timestamp ts=-1, te=-1;

	void process(const dds_record& rec);
	void finish();
	void report(std::ostream& s);

public:
	data_source_statistics(); 
	~data_source_statistics();
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
	: Q(_Q), series("hist_"s+repr(Q), string("%.0f").c_str())
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

/*
	This holds and updates incrementally 
	an AGMS sketch on a specific
	stream.
*/
struct agms_sketch_updater : reactive
{
	stream_id sid;
	agms::incremental_sketch isk;

	agms_sketch_updater(stream_id _sid, agms::projection proj)
	: sid(_sid), isk(proj)
	{
		on(START_RECORD, [&]() {
			const dds_record& rec = CTX.stream_record();
			if(rec.sid==sid) {
				isk.update(rec.key, (rec.sop==INSERT)?1.0:-1.0);
				emit(STREAM_SKETCH_UPDATED);
			}
		});
	}
};

// Factory
extern factory<agms_sketch_updater, stream_id, agms::projection> 
	agms_sketch_updater_factory;


template <>
inline agms_sketch_updater* 
inject<agms_sketch_updater, stream_id, agms::projection>(stream_id sid, 
	agms::projection proj)
{
	return agms_sketch_updater_factory(sid, proj);
}


/*
	Base for AGMS query estimators
 */
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
		s << "agms_" << repr(q);
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


/*
	Self-join query estimator. It uses an incremental_updated
 */
class selfjoin_agms_method : public agms_method<qtype::SELFJOIN>
{
	agms::incremental_norm2 norm2_estimator;

	void process_record();
	void finish();
public:
	selfjoin_agms_method(stream_id sid, agms::depth_type D, agms::index_type L);
};


/*
	Join query estimator. It uses 2 incremental_updater objects.
 */
class twoway_join_agms_method : public agms_method<qtype::JOIN>
{
	agms::incremental_prod prod_estimator;

	// callbacks
	void process_record();
	void finish();
public:
	twoway_join_agms_method(stream_id s1, stream_id s2, agms::depth_type D, agms::index_type L);
};




} // end namespace dds

#endif