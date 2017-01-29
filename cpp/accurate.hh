#ifndef __ACCURATE_H__
#define __ACCURATE_H__

#include <iostream>
#include <set>

#include "dds.hh"
#include "method.hh"
#include "mathlib.hh"

using dds::timestamp;
using dds::stream_id;
using dds::source_id;
using dds::local_stream_id;
using dds::distinct_histogram;
using std::set;
using dds::dds_record;

class data_source_statistics : public dds::exec_method
{
	set<stream_id> sids;
	set<source_id> hids;

	distinct_histogram<local_stream_id> lshist;

	size_t scount=0;
	timestamp ts=-1, te=-1;
public:
	void process(const dds_record& rec) override;
	void finish() override;

	void report(std::ostream& s);
};



#endif