
#include <iomanip>
#include "accurate.hh"

using namespace dds;

data_source_statistics::data_source_statistics()
{
	stream_size.resize(dds::MAX_SID);
	this->on(START_RECORD, [&](){ process(CTX.stream_record()); });
	this->on(END_STREAM, [&](){ finish(); });
}

void data_source_statistics::process(const dds_record& rec)
{
	if(scount==0) ts=rec.ts;
	te = rec.ts;
	sids.insert(rec.sid);
	hids.insert(rec.hid);
	stream_size.add(rec.sid);
	//std::cout << rec << std::endl;
	lshist.add(rec.local_stream());
	scount++;
}


void data_source_statistics::finish()
{
	report(std::cout);
}

void data_source_statistics::report(std::ostream& s)
{
	using std::endl;
	using std::setw;

	s 	<< "Stats:" << scount 
		<< " streams=" << sids.size() 
		<< " local hosts=" << hids.size() << endl;

	const int CW = 9;
	const int NW = 10;

	// header
	s << setw(CW) << "Stream:";
	for(auto sid : sids) 
		s << setw(NW) << sid;
	s << endl;

	for(auto hid: hids) {
		s << "host "<< setw(3) << hid << ":";
		for(auto sid: sids) 
			s << setw(NW) << lshist[std::make_pair(sid,hid)];
		s << endl;
	}

	for(auto sid=stream_size.begin();sid!=stream_size.end();sid++) {
		s << "stream[" << sid.index() << "]=" << *sid << endl;
	}
}


//
//////////////////////////////////////////////
//


selfjoin_exact_method::selfjoin_exact_method(stream_id sid)
: exact_method<qtype::SELFJOIN>(self_join(sid)) 
{ 
	on(START_RECORD, [&](){ process_record(CTX.stream_record()); });
	on(END_STREAM, [&](){  finish(); });
}

void selfjoin_exact_method::process_record(const dds_record& rec)
{
	if(rec.sid == Q.param) {
		size_t x;
		if(rec.sop == INSERT) {
			x = histogram.add(rec.key);
			curest += 2*x + 1;
		}
		else {
			x = histogram.erase(rec.key);
			curest -= 2*x - 1;
		}
	}
	series = curest;
}

void selfjoin_exact_method::finish()
{ 
	cout << "selfjoin(" << Q.param << ")=" << curest << endl;
}



//
//////////////////////////////////////////////
//

twoway_join_exact_method::twoway_join_exact_method(stream_id s1, stream_id s2)
: exact_method<qtype::JOIN>(join(s1,s2)) 
{ 
	on(START_RECORD, [=](){ process_record(CTX.stream_record()); 
	});
	on(END_STREAM, [=](){
		finish();
	});
}

// update goes in h1
void twoway_join_exact_method::
	dojoin(histogram& h1, histogram& h2, const dds_record& rec)
{
	size_t& x = h1.get_counter(rec.key);
	size_t& y = h2.get_counter(rec.key);

	if(rec.sop == INSERT) {
		x += 1;
		curest += y;		
	} else {
		x -= 1;
		curest -= y;				
	}
}

void twoway_join_exact_method::process_record(const dds_record& rec)
{
	if(rec.sid == Q.param.first) {
		dojoin(hist1, hist2, rec);
	} else if(rec.sid == Q.param.second) {
		dojoin(hist2, hist1, rec);		
	} 
	// else, discard the sample
	series = curest;
}

void twoway_join_exact_method::finish()
{ 
	cout << "2wayjoin(" 
		<< Q.param.first << "," << Q.param.second << ")=" 
		<< curest << endl;
}


//
//////////////////////////////////////////////
//

factory<agms_sketch_updater, stream_id, agms::projection>
	dds::agms_sketch_updater_factory ;


selfjoin_agms_method::selfjoin_agms_method(stream_id sid, 
	depth_type D, index_type L
	) : agms_method<qtype::SELFJOIN>(self_join(sid), D, L), 
		norm2_estimator(
			& agms_sketch_updater_factory(sid, 
				agms::projection(D,L))
				->isk
		)
{ 
	on(STREAM_SKETCH_UPDATED, [&](){ process_record(); });
	on(END_STREAM, [&](){  finish(); });
}

void selfjoin_agms_method::process_record()
{
	if(CTX.stream_record().sid==Q.param) {
		norm2_estimator.update_incremental();
		curest = norm2_estimator.median_estimate();
	}
	series = curest;
}

void selfjoin_agms_method::finish()
{ 
	cout << Q << "=" << curest << endl;
}


//
//////////////////////////////////////////////
//

twoway_join_agms_method::twoway_join_agms_method(
	stream_id s1, stream_id s2, agms::depth_type D, agms::index_type L
	) : agms_method<qtype::JOIN>(join(s1,s2), D, L),
		prod_estimator(
			& agms_sketch_updater_factory(s1, 
				agms::projection(D,L))
				->isk
			,
			& agms_sketch_updater_factory(s2, 
				agms::projection(D,L))
				->isk
		)
	{
		on(STREAM_SKETCH_UPDATED, [&](){ process_record(); });
		on(END_STREAM, [&](){  finish(); });
	}

void twoway_join_agms_method::process_record()
{
	if(CTX.stream_record().sid==Q.param.first) {
		prod_estimator.update_incremental_1();
		curest = prod_estimator.median_estimate();
	} else if(CTX.stream_record().sid==Q.param.second) {
		prod_estimator.update_incremental_2();
		curest = prod_estimator.median_estimate();		
	}
	series = curest;
}

void twoway_join_agms_method::finish()
{ 
	cout << Q << "=" << curest << endl;
}

