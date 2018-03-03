#ifndef _DDS_HH_
#define _DDS_HH_

#include <iostream>
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


inline bool operator==(const dds_record& rec1, const dds_record& rec2)
{
	return rec1.sid==rec2.sid 
		&& rec1.hid==rec2.hid 
		&& rec1.key==rec2.key 
		&& rec1.upd==rec2.upd 
		&& rec1.ts==rec2.ts;
}


inline bool before(const dds_record& r1, const dds_record& r2)
{
	return r1.ts < r2.ts;
}

ostream& operator<<(ostream& s, dds_record const & rec);





} // end namespace dds


//
// Definition of byte sizes
//
#include "dsarch_types.hh"


namespace dsarch
{
	using namespace dds;	

//BYTE_SIZE_SIZEOF(key_type) 
//BYTE_SIZE_SIZEOF(counter_type) 
//BYTE_SIZE_SIZEOF(timestamp) 
//BYTE_SIZE_SIZEOF(stream_id) 
BYTE_SIZE_SIZEOF(source_id) 
BYTE_SIZE_SIZEOF(dds_record)

}




#endif