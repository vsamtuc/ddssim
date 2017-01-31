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

class data_source_statistics : public dds::exec_method
{
	set<stream_id> sids;
	set<source_id> hids;

	distinct_histogram<local_stream_id> lshist;
	frequency_vector<stream_id> stream_size;

	size_t scount=0;
	timestamp ts=-1, te=-1;
public:

	data_source_statistics() {
		stream_size.resize(dds::MAX_SID);
	}

	void process(const dds_record& rec) override;
	void finish() override;

	void report(std::ostream& s);
};


using std::map;


class selfjoin_exact_method : public dds::estimating_method
{
	const self_join Q;

	distinct_histogram<key_type> histogram;
	double curest = 0.0;
public:
	selfjoin_exact_method(stream_id sid);


	void process_record(const dds_record& rec) override;
	void finish() override;

	const basic_query& query() const;
	double current_estimate() const override;

};


} // end namespace dds

#endif