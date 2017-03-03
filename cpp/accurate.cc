
#include <iomanip>
#include "accurate.hh"

using namespace dds;

data_source_statistics::data_source_statistics()
{
	stream_size.resize(dds::MAX_SID);
	on(START_RECORD, [&](){ process(CTX.stream_record()); });
	on(RESULTS, [&](){ finish(); });

	for(auto sid : CTX.metadata().stream_ids())
		for(auto hid : CTX.metadata().source_ids())
		{
			char buf[32];
			sprintf(buf, "LSSize_s%d_h%d", sid, hid);
			col_t *c = new col_t(buf, "%d", 0);
			lssize[local_stream_id {sid, hid}] = c;
			CTX.timeseries.add(*c);
		}

	for(auto sid : CTX.metadata().stream_ids()) 
	{
		char buf[32];
		sprintf(buf, "SSize_s%d", sid);
		col_t *c = new col_t(buf, "%d", 0);
		ssize[sid] = c;
		CTX.timeseries.add(*c);
	}

	for(auto hid : CTX.metadata().source_ids())
	{
		char buf[32];
		sprintf(buf, "SSize_h%d", hid);
		col_t *c = new col_t(buf, "%d", 0);
		hsize[hid] = c;
		CTX.timeseries.add(*c);
	}

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
	int delta = (rec.sop==INSERT)?1:-1;
	lssize[rec.local_stream()]->value() += delta;
	ssize[rec.sid]->value() += delta;
	hsize[rec.hid]->value() += delta;
	scount++;
}


void data_source_statistics::finish()
{
	report(std::cout);
}


data_source_statistics::~data_source_statistics()
{
	// clear timeseries columns
	for(auto i : lssize)
		delete i.second;
	for(auto i : ssize)
		delete i.second;
	for(auto i : hsize)
		delete i.second;
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
		for(auto sid: sids) {
			auto stream_len = lshist[std::make_pair(sid,hid)]; 

			local_stream_stats.sid = sid;
			local_stream_stats.hid = hid;
			local_stream_stats.stream_len = stream_len;
			local_stream_stats.emit_row();

			s << setw(NW) << stream_len;
		}
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
	depth_type D, index_type L) 
: selfjoin_agms_method(sid, agms::projection(D,L))
{}

selfjoin_agms_method::selfjoin_agms_method(stream_id sid, 
	const agms::projection& proj) 
: agms_method<qtype::SELFJOIN>(self_join(sid))
{ 
	using namespace agms;

	isk = & agms_sketch_updater_factory(sid, proj)->isk;
	curest = dot_est_with_inc(incstate, *isk);

	on(STREAM_SKETCH_UPDATED, [&](){ process_record(); });
}

void selfjoin_agms_method::process_record()
{
	using namespace agms;

	if(CTX.stream_record().sid==Q.param) {
		curest = dot_est_inc(incstate, isk->delta);
	}
}


//
//////////////////////////////////////////////
//


twoway_join_agms_method::twoway_join_agms_method(stream_id s1, stream_id s2, 
	agms::depth_type D, agms::index_type L) 
: twoway_join_agms_method(s1, s2, agms::projection(D,L))
{ }

twoway_join_agms_method::twoway_join_agms_method(stream_id s1, stream_id s2, 
	const agms::projection& proj) 
: agms_method<qtype::JOIN>(join(s1,s2))
{
	using namespace agms;
	isk1 = & agms_sketch_updater_factory(s1, proj)->isk;
	isk2 = & agms_sketch_updater_factory(s2, proj)->isk;
	curest = dot_est_with_inc(incstate, *isk1, *isk2);

	on(STREAM_SKETCH_UPDATED, [&](){ process_record(); });
}

void twoway_join_agms_method::process_record()
{
	using namespace agms;
	if(CTX.stream_record().sid==Q.param.first) {
		curest = dot_est_inc(incstate, isk1->delta, *isk2);
	} else if(CTX.stream_record().sid==Q.param.second) {
		curest = dot_est_inc(incstate, *isk1, isk2->delta);
	}
}

