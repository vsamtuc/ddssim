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
#include "query.hh"

namespace dds {

using std::set;
using std::map;
using std::vector;
using std::cout;
using std::endl;

using namespace std::string_literals;

class data_source_statistics : public reactive
{
	set<stream_id> sids;
	set<source_id> hids;

	// count total size of local streams
	frequency_vector<local_stream_id> lshist;

	// count each stream size
	frequency_vector<stream_id> stream_size;

	// timeseries for 'active' records per local stream
	typedef column<long int> col_t;
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

	static component_type<data_source_statistics> comp_type; 
};


using std::map;


/*************************************
 *
 *  Query Estimation
 *
 *************************************/


class query_method : public reactive
{
protected:
	basic_stream_query Q;
	string method_name;
	double curest = 0.0;

	column_ref<double> series;
public:
	query_method(const basic_stream_query& _Q, const string& methname) 
	: Q(_Q), method_name(methname), series({methname+"_"+repr(Q), "%.0f", curest})
	{
		CTX.timeseries.add(series);
	}

	const basic_stream_query& query() const { return Q; }

	inline double current_estimate() const { return curest; }
};



/*************************************
 *
 *  Methods based on histgrams
 *
 *************************************/



class selfjoin_exact_method : public query_method
{
	frequency_vector<key_type> histogram;

	void process_record(const dds_record& rec);
	void process_warmup(const buffered_dataset& wset);
	void finish();
public:
	selfjoin_exact_method(stream_id sid);

};


class twoway_join_exact_method : public query_method
{
	typedef frequency_vector<key_type> histogram;
	histogram hist1;
	histogram hist2;

	// helper
	void dojoin(histogram& h1, histogram& h2, const dds_record& rec);
	// callbacks
	void process_warmup(const buffered_dataset& wset);
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
	agms::isketch isk;

	agms_sketch_updater(stream_id _sid, agms::projection proj);
};

// Factory
extern factory<agms_sketch_updater, stream_id, agms::projection> 
	agms_sketch_updater_factory;




/*
	Self-join query estimator. 
 */
class selfjoin_agms_method : public query_method
{
	agms::isketch* isk;
	Vec incstate;
	bool isinit = false;

	void initialize();
	void process_record();
public:
	selfjoin_agms_method(stream_id sid, const agms::projection& proj);
	selfjoin_agms_method(stream_id sid, agms::depth_type D, agms::index_type L);

};


/*
	Join query estimator.
 */
class twoway_join_agms_method : public query_method
{
	agms::isketch *isk1, *isk2;
	Vec incstate;
	bool isinit = false;

	// callbacks
	void initialize();
	void process_record();
public:
	twoway_join_agms_method(stream_id s1, stream_id s2, const agms::projection& proj);
	twoway_join_agms_method(stream_id s1, stream_id s2, agms::depth_type D, agms::index_type L);

};


extern component_type<query_method> exact_query_comptype;
extern component_type<query_method> agms_query_comptype;



} // end namespace dds

#endif
