#ifndef _DDS_HH_
#define _DDS_HH_

#include <iostream>

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

	inline void repr(ostream& s) const {
		s 
			<< "<"
			<< sid << ","
			<< (sop==INSERT ? "INS[" : "DEL[") << key << "],"
			<< "ts="<< ts << " "
			<< "at "<< hid
			<< ">";
	}	
};



inline ostream& operator<<(ostream& s, const dds_record& rec)
{
	rec.repr(s);
	return s;
}


} // end namespace dds


#endif