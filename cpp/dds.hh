#ifndef _DDS_HH_
#define _DDS_HH_

#include <iostream>
#include <utility>
#include <string>
#include <set>
#include <limits>

namespace dds {

/// The key type for a stream record
typedef int32_t  key_type;

/// The maximum key value
constexpr key_type MAX_KEY = std::numeric_limits<key_type>::max();
constexpr key_type MIN_KEY = std::numeric_limits<key_type>::min();

/// The timestamp for a stream record
typedef int32_t  timestamp;
constexpr key_type MAX_TS = std::numeric_limits<timestamp>::max();
constexpr key_type MIN_TS = std::numeric_limits<timestamp>::min();

/// The operation type
enum stream_op : int8_t {
	INSERT,
	DELETE
};

/// The id of a stream 
typedef int16_t stream_id;
constexpr key_type MAX_SID = std::numeric_limits<stream_id>::max();
constexpr key_type MIN_SID = (stream_id)0;

/// The id of a distributed stream source
typedef int16_t source_id;
constexpr key_type MAX_HID = std::numeric_limits<source_id>::max();
constexpr key_type MIN_HID = (source_id)0;


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
	Data stream metadata.

	This is stuff needed by the monitoring algorithms
  */
class ds_metadata
{
protected:
	set<stream_id> sids;
	set<source_id> hids;

	size_t scount=0;
	timestamp ts=0, te=0;
	key_type kmin = MAX_KEY, kmax=MIN_KEY;
public:
	inline void collect(const dds_record& rec) {
		if(scount==0) ts=rec.ts;
		te = rec.ts;
		sids.insert(rec.sid);
		hids.insert(rec.hid);
		if(rec.key<kmin) kmin=rec.key;
		if(rec.key>kmax) kmax=rec.key;
		scount++;
	}

	inline size_t size() const { return scount; }
	inline timestamp mintime() const { return ts; }
	inline timestamp maxtime() const { return te; }
	inline key_type minkey() const { return kmin; }
	inline key_type maxkey() const { return kmax; }	
	inline auto stream_ids() const { return sids; }
	inline auto source_ids() const { return hids; }
};


} // end namespace dds


#endif