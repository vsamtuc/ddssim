
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <stdexcept>

#include <boost/format.hpp>
#include <boost/endian/conversion.hpp>

#include "data_source.hh"


using namespace std;
using namespace dds;

// Time Window

time_window_source::time_window_source(data_source* _sub, dds::timestamp _w)
	: sub(_sub), Tw(_w) 
	{
		advance();
	}

void time_window_source::advance()
{
	if(!isvalid) return;
	if(sub->valid() && !window.empty()) {
		if(sub->get().ts > window.front().ts)
			advance_from_window();
		else
			advance_from_sub();
	} else if(sub->valid())
			advance_from_sub();
	else if(! window.empty())
		advance_from_window();
	else
		isvalid = false;
}

void time_window_source::advance_from_window()
{
	rec = window.front();
	window.pop();
}

void time_window_source::advance_from_sub()
{
	rec = sub->get();
	sub->advance();
	window.push(rec);
	window.back().sop = dds::DELETE;
	window.back().ts += Tw;
}


// Fixed window




//-----------------------------
//	Crawdad
//-----------------------------


struct cio_error : runtime_error
{
	cio_error(const char* func, int rc, int errsv)
		: runtime_error(boost::str(
            	boost::format("In %s: rc=%d\n errno=%d. %s\n") 
            	% func % rc % errsv % std::strerror(errsv)
			)) {}
};


struct crawdad_record {
    char siteString[100];
    char day[100];
    char moment[100];
    char parent[100];
    unsigned int aid;
    char state[100];
    unsigned int shortRet;
    unsigned int longRet;
    int strength;
    int quality;
    char mac[100];
    char classId[100];
    unsigned int srcPkts;
    unsigned int srcOct;
    unsigned int srcErrPkts;
    unsigned int srcErrOct;
    unsigned int dstPkts;
    unsigned int dstOct;
    unsigned int dstErrPkts;
    unsigned int dstErrOct;
    unsigned int dstMaxRetryErr;
    char ip[100];

    bool read(FILE* stream) {
        int rc = fscanf(stream, 
        	"%s %s %s %s %d %s %d %d %d %d %s %s %d %d %d %d %d %d %d %d %d %s\n",
                siteString, day, moment, parent, &(this->aid), state,
                &(this->shortRet), &(this->longRet), &(this->strength), 
                &(this->quality), mac,
                classId, &(this->srcPkts), &(this->srcOct), 
                &(this->srcErrPkts), &(this->srcErrOct),
                &(this->dstPkts), &(this->dstOct), &(this->dstErrPkts), 
                &(this->dstErrOct), &(this->dstMaxRetryErr),
                ip);
        if(rc != 22) {
            int errsv = errno;
            throw cio_error(__FUNCTION__, rc, errsv);
        }
        return true;
    }

	inline dds::stream_id stream() const { return (siteString[0]=='L') ? 0 : 1; }

	inline dds::source_id site() const { return aid-29; }

	inline dds::key_type value() const { return shortRet; }

	static inline dds::timestamp date2time(int yr, int mo, int day, int hr, int min, int sec)
	{
		return sec + 60l*min + 3600l*hr + 86400l*(365l*yr+31l*mo+day-31l);
	}

	const dds::timestamp dataset_base_tstamp = date2time(2, 7, 20, 0, 0, 0);

	dds::timestamp tstamp() const {
		int rc;
		int yr, mo, da, hr, min, sec;

		// read the day
		rc = sscanf(day, "%d-%d-%d", &yr, &mo, &da);
		if(rc!=3) {
			int errsv = errno;
            throw cio_error(__FUNCTION__, rc, errsv);
		}

		// read the hour
		rc = sscanf(moment, "%d:%d:%d", &hr, &min, &sec);
		if(rc!=3) {
		    int errsv = errno;
            throw cio_error(__FUNCTION__, rc, errsv);
		}

		return date2time(yr,mo,da,hr,min,sec) - dataset_base_tstamp;
	}

};



struct wcup_record 
{
public:
    uint32_t timestamp;
    uint32_t clientID;
    uint32_t objectID;
    uint32_t size;
    uint8_t method;
    uint8_t status;
    uint8_t type;
    uint8_t server;

    static inline uint32_t twiddle(uint32_t in)
	{
	   return(( in >> 24 )  | 
	          (( in & ((1<<8)-1) << 16) >> 8 ) |
	          (( in & ((1<<8)-1) <<  8) << 8 ) |  
	          (( in & ((1<<8)-1)) << 24));
	}
    
    bool read(FILE *stream)
    {
		static_assert(sizeof(*this)==20, "Error! cannot match record size!");
		size_t rc = fread((void*)this, 20, 1, stream);

        if(rc != 1) {
        	if(feof(stream))
        		return false;
            int errsv = errno;
            throw cio_error(__FUNCTION__, rc, errsv);
        }

  		// Fix endianness
#define FIX_ENDIAN(attr)  boost::endian::big_to_native_inplace(attr)
//#define FIX_ENDIAN(attr)  attr = twiddle(attr)

        FIX_ENDIAN(timestamp);
        FIX_ENDIAN(clientID);
        FIX_ENDIAN(objectID);
        FIX_ENDIAN(size);
        return true;
    }


	inline dds::stream_id stream() const { return type; }

	inline dds::source_id site() const { return server; }

	inline dds::key_type value() const { return clientID; }

	inline dds::timestamp tstamp() const { return timestamp; }
};



template <typename FileRecord>
class file_data_source : public data_source
{
protected:
	string filepath;
	FILE* fstream;
	FileRecord record;
public:
	file_data_source(const string& fpath, const char* mode) 
	: filepath(fpath), fstream(0)
	{
		fstream = fopen(filepath.c_str(), mode);
		if(! fstream) {
			throw cio_error(__FUNCTION__, 0, errno);			
		}
		advance();
	}

	~file_data_source() 
	{
		if(fstream) {
			fclose(fstream);
		}
	}

	void advance() 
	{
		if(feof(fstream)) {
			isvalid = false;
		} else {
			isvalid = record.read(fstream);
			if(!isvalid) return;
			
			// populate the dds record
			rec.sid = record.stream();
			rec.hid = record.site();
			rec.sop = dds::INSERT;
			rec.ts = record.tstamp();
			rec.key = record.value();
		}
	}

};


data_source* dds::crawdad_ds(const string& fpath) 
{
	return new file_data_source<crawdad_record>(fpath, "r");
}

data_source* dds::wcup_ds(const string& fpath) 
{
	return new file_data_source<wcup_record>(fpath, "r");
}


void buffered_dataset::analyze(ds_metadata& meta) const
{
	for(auto& rec : *this) {
		meta.collect(rec);
	}
}

void buffered_dataset::load(data_source* src) 
{
	for(;src->valid();src->advance())
		this->push_back(src->get());
}


buffered_data_source::buffered_data_source(buffered_dataset& dset)
: buffer(dset), from(dset.begin()), to(dset.end())
{
	buffer.analyze(dsm);
	advance();
}

buffered_data_source::buffered_data_source(buffered_dataset& dset,
	const ds_metadata& meta)
: buffer(dset), dsm(meta), from(dset.begin()), to(dset.end())
{
	advance();
}


void buffered_data_source::advance()
{
	if(! isvalid) return;
	if(from != to) {
		rec = *from;
		from ++;
	} else {
		isvalid = false;
	}
}

