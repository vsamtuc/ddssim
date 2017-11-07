#ifndef _DDS_HH_
#define _DDS_HH_

#include <iostream>
#include <sstream>
#include <utility>
#include <string>
#include <set>
#include <iterator>
#include <limits>
#include <array>
#include <cassert>

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

/// The stream counter type
typedef int32_t  counter_type;
constexpr counter_type MAX_COUNTER = std::numeric_limits<counter_type>::max();
constexpr counter_type MIN_COUNTER = std::numeric_limits<counter_type>::min();

/// The id of a stream 
typedef int16_t stream_id;
constexpr key_type MAX_SID = std::numeric_limits<stream_id>::max();
constexpr key_type MIN_SID = (stream_id)0;

/// The id of a distributed stream source
typedef int16_t source_id;
constexpr key_type MAX_HID = std::numeric_limits<source_id>::max();
constexpr key_type MIN_HID = (source_id)0;



using std::ostream;

/**
	A local stream id combines a `stream_id` and a `source_id`.

	Its size is 4 bytes.
  */
struct local_stream_id
{
	stream_id sid;  		/// stream id
	source_id hid;			/// local host id
};




inline bool operator<(local_stream_id lsid1, local_stream_id lsid2)
{
	return lsid1.hid < lsid2.hid || (lsid1.hid==lsid2.hid && lsid1.sid < lsid2.sid);
}

inline bool operator==(local_stream_id lsid1, local_stream_id lsid2)
{
	return lsid1.hid == lsid2.hid && lsid1.sid == lsid2.sid;
}
inline bool operator!=(local_stream_id lsid1, local_stream_id lsid2) {
	return !(lsid1 == lsid2);
}



/**
 	A stream update contains a key and a counter.

 	Its size is 8 bytes.
  */
struct stream_update
{
	key_type key;			/// record key
	counter_type upd;		/// the update
};


/**
	A distributed stream tuple.

	This object combines a local stream id, a stream update and
	a timestamp. Its total size is 16 bytes.
  */
struct dds_record 
{
	stream_id sid;  		/// stream id
	source_id hid;			/// local host id
	key_type key;			/// record key
	counter_type upd;		/// the update	
	timestamp ts;			/// timestamp


	inline local_stream_id local_stream() const { 
		return local_stream_id { sid, hid };
	}
	inline operator local_stream_id() const { return {sid, hid}; }

	inline stream_update update() const { 
		return stream_update {key, upd}; 
	}
	inline operator stream_update() const {
		return { key, upd }; 
	}

	inline void repr(ostream& s) const {
		s 
			<< "<s="<< sid 
			<< ",h="<< hid
			<< ",["	<< key << (upd>=0 ? "]+" : "]") << upd
			<< ",t="<< ts
			<< ">";
	}	

	static const dds_record zero; 
};



inline bool before(const dds_record& r1, const dds_record& r2)
{
	return r1.ts < r2.ts;
}

ostream& operator<<(ostream& s, dds_record const & rec);


/**
	Named objects are just used to enable human-readable reporting
  */
class named
{
	mutable std::string n;
public:
	/// Make a name from a pointer
	static std::string anon(named const * ptr);

	named() : n() { }
	named(const std::string& _n) : n(_n) {}
	virtual ~named() {}
	inline void set_name(const std::string& _name) { n=_name; }
	inline const std::string& name() const { 
		if(n.empty()) 
			n = anon(this);
		return n; 
	}
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
	std::string dsname="<anon>";
	timestamp dswindow=0;
	timestamp dswarmup_time=0;
	size_t dswarmup_size=0;

	bool isvalid = false;

	set<stream_id> sids;
	set<source_id> hids;

	size_t scount=0;
	timestamp ts=MAX_TS, te=MIN_TS;
	key_type kmin = MAX_KEY, kmax=MIN_KEY;
public:

	inline const std::string& name() const { return dsname; }
	inline void set_name(const std::string& _n) { dsname=_n; }

	inline timestamp window() const { return dswindow; }
	inline void set_window(timestamp w) { dswindow=w; }

	inline size_t warmup_size() const { return dswarmup_size; }
	inline void set_warmup_size(size_t w) { dswarmup_size = w; }

	inline timestamp warmup_time() const { return dswarmup_time; }
	inline void set_warmup_time(timestamp w) { dswarmup_time = w; }

	inline bool valid() const { return isvalid; }
	inline void set_valid(bool v=true) { isvalid=v; }

	inline void prepare_collect()
	{
		scount=0;
		ts=MAX_TS; te=MIN_TS;
		kmin = MAX_KEY; kmax=MIN_KEY;		
	}

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
	inline timestamp duration() const { return te-ts+1; }

	inline timestamp mintime() const { return ts; }
	inline timestamp maxtime() const { return te; }

	inline key_type minkey() const { return kmin; }
	inline key_type maxkey() const { return kmax; }	

	inline const set<stream_id>& stream_ids() const { return sids; }
	inline const set<source_id>& source_ids() const { return hids; }


	// mutable interface
	void set_size(size_t s) { scount = s; }
	void set_ts_range(timestamp _ts, timestamp _te) { ts=_ts; te=_te; }
	void set_key_range(key_type _kmin, key_type _kmax) { kmin=_kmin; kmax=_kmax; }

	void set_stream_ids(const set<stream_id>& _sids) { sids = _sids; }
	void set_source_ids(const set<source_id>& _hids) { hids = _hids; }

	template <typename Iter>
	void set_stream_range(Iter from, Iter to) {
		sids.clear();
		std::copy(from, to, std::inserter(sids, sids.end()));
	}

	template <typename Iter>
	void set_source_range(Iter from, Iter to) {
		hids.clear();
		std::copy(from, to, std::inserter(hids, hids.end()));
	}

	void merge(const ds_metadata& other);

};





/*-----------------------------------

	Descriptors for global queries

  -----------------------------------*/

enum class qtype 
{
	VOID,
	SELFJOIN,
	JOIN
};

struct basic_query
{
	qtype type;
	constexpr basic_query() : type(qtype::VOID) {}
	constexpr basic_query(qtype t) : type(t) {}
};

	namespace __traits {

		template <qtype Type> struct query_traits;
		template <> struct query_traits<qtype::VOID> 
		{
			static const qtype query_type = qtype::VOID;
			typedef void param_type;
		};
		template <> struct query_traits<qtype::SELFJOIN> 
		{
			static const qtype query_type = qtype::SELFJOIN;
			typedef stream_id param_type;
		};
		template <> struct query_traits<qtype::JOIN> 
		{
			static const qtype query_type = qtype::JOIN;
			typedef std::pair<stream_id, stream_id> param_type;
		};

	}

template <qtype Type>
struct typed_query : basic_query
{
	typedef __traits::query_traits<Type> traits;
	typedef typename traits::param_type param_type;

	param_type param;

	constexpr typed_query() 
	: basic_query(Type), param() {}
	constexpr typed_query(const param_type& p) 
	: basic_query(Type), param(p) {}
};

template <>
struct typed_query<qtype::VOID> : basic_query
{
	typedef __traits::query_traits<qtype::VOID> traits;
	typedef typename traits::param_type param_type;

	constexpr typed_query() 
	: basic_query(qtype::VOID) {}
};


template <qtype Type>
inline typed_query<Type>& query_cast(basic_query& q) 
{
	assert(q.type == Type);
	return static_cast< typed_query<Type>& >(q);
}
template <qtype Type>
inline const typed_query<Type>& query_cast(const basic_query& q) 
{
	assert(q.type == Type);
	return static_cast< const typed_query<Type>& >(q);
}

// query relational operators
bool operator==(const basic_query& q1, const basic_query& q2);

inline bool operator!=(const basic_query& q1, const basic_query& q2)
{
	return ! (q1==q2);
}

// Short names for queries
using self_join = typed_query<qtype::SELFJOIN>;
using twoway_join = typed_query<qtype::JOIN>;

inline auto join(stream_id s1, stream_id s2) {
	return twoway_join(std::make_pair(s1,s2));
}


std::ostream& operator<<(std::ostream& s, const basic_query& q);
std::ostream& operator<<(std::ostream& s, qtype qt);

inline std::string repr(const basic_query& q)
{
	std::ostringstream S;
	S << q;
	return S.str();
}



/*-----------------------------------

	Byte size for 

  -----------------------------------*/


/**
	By default, types with a "byte_size" method are handled.
  */
template <typename MsgType>
size_t byte_size(const MsgType& m)
{
	return m.byte_size();
}

/**
	Byte size of types, when serialized for transmission
  */
template <>
inline size_t byte_size<std::string>(const std::string& s)
{
	return s.size();
}

#define BYTE_SIZE_SIZEOF(type)\
template<>\
inline size_t byte_size<type>(const type& i) { return sizeof(type); }

BYTE_SIZE_SIZEOF(source_id) // covers stream_id also
BYTE_SIZE_SIZEOF(int)
BYTE_SIZE_SIZEOF(unsigned int)
BYTE_SIZE_SIZEOF(long)
BYTE_SIZE_SIZEOF(unsigned long)
BYTE_SIZE_SIZEOF(float)
BYTE_SIZE_SIZEOF(double)

BYTE_SIZE_SIZEOF(dds_record)

} // end namespace dds





#endif