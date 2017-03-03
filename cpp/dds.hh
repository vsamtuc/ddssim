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

ostream& operator<<(ostream& s, dds_record const & rec);


/**
	Named objects are just used to enable human-readable reporting
  */
class named
{
	std::string n;
public:
	/// Make a name from a pointer
	static std::string anon(named* ptr);

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
	inline const set<stream_id>& stream_ids() const { return sids; }
	inline const set<source_id>& source_ids() const { return hids; }

	// mutable interface
	void set_size(size_t s) { scount = s; }
	void set_ts_range(timestamp _ts, timestamp _te) { ts=_ts; te=_te; }
	void set_key_range(key_type _kmin, key_type _kmax) { kmin=_kmin; kmax=_kmax; }

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