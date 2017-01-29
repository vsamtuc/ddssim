
#include <iomanip>
#include "accurate.hh"


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