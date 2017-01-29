#ifndef _DDS_HH_
#define _DDS_HH_

#include <iostream>
#include <utility>
#include <string>
#include <set>

namespace dds {

/// The key type for a stream record
typedef long long int  key_type;

/// The timestamp for a stream record
typedef int64_t  timestamp;

/// The operation type
enum stream_op {
	INSERT,
	DELETE
};

/// The id of a stream 
typedef int stream_id;

/// The id of a distributed stream source
typedef int source_id;


typedef std::pair<stream_id, source_id> local_stream_id;

using std::ostream;

/**
	A stream tuple with a single key. 
  */
struct dds_record
{
	stream_id sid;  /// stream id
	source_id hid;	/// local host id
	stream_op sop;	/// stream operation
	timestamp ts;	/// timestamp
	key_type key;	/// record key

	inline local_stream_id local_stream() const { 
		return std::make_pair(sid, hid);
	}

	inline void repr(ostream& s) const {
		s 
			<< "<"
			<< sid << ","
			<< (sop==INSERT ? "INS[" : "DEL[") << key << "],"
			<< "ts="<< ts << " "
			<< "at "<< hid
			<< ">";
	}	

	static const dds_record zero; 
};

inline dds_record make_record(stream_id sid, source_id hid, 
	stream_op sop, timestamp ts, key_type key)
{
	return dds_record {sid, hid, sop, ts, key};
}

inline bool before(const dds_record& r1, const dds_record& r2)
{
	return r1.ts < r2.ts;
}

inline ostream& operator<<(ostream& s, const dds_record& rec)
{
	rec.repr(s);
	return s;
}


/**
	Named objects are just used to enable human-readable reporting
  */
class named
{
	std::string n;
public:
	/// Make a name from a pointer
	static std::string anon(void* ptr);

	named() : n(anon(this)) { }
	named(const std::string& _n) : n(_n) {}
	inline void set_name(const std::string& _name) { n=_name; }
	inline const std::string& name() const { return n; }
};


inline ostream& operator<<(ostream& s, const named& obj)
{
	return s << obj.name();
}

using std::set;

/**
	Data source metadata.

	This is stuff needed by the 
  */
class ds_metadata
{
protected:
	set<stream_id> sids;
	set<source_id> hids;

	size_t scount=0;
	timestamp ts=-1, te=-1;
public:
	inline void collect(const dds_record& rec) {
		if(scount==0) ts=rec.ts;
		te = rec.ts;
		sids.insert(rec.sid);
		hids.insert(rec.hid);
		scount++;
	}

	inline size_t size() const { return scount; }
	inline timestamp mintime() const { return ts; }
	inline timestamp maxtime() const { return te; }
	inline auto stream_ids() const { return sids; }
	inline auto source_ids() const { return hids; }
};


} // end namespace dds


#endif