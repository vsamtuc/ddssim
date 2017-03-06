
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <stdexcept>

#include <boost/format.hpp>
#include <boost/endian/conversion.hpp>

#include "data_source.hh"
#include "hdf5_util.hh"
#include "binc.hh"

using namespace std;
using namespace dds;

// Time Window

time_window_source::time_window_source(datasrc _sub, dds::timestamp _w)
	: sub(_sub), Tw(_w) 
	{
		set_metadata(sub->metadata());

		// double the size
		dsm.set_size( dsm.size() );

		// adjust the limits
		dsm.set_ts_range( 
			min(dsm.mintime(), dsm.mintime()+Tw),
			max(dsm.maxtime(), dsm.maxtime()+Tw)
		  );

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
	window.back().upd = -window.back().upd;
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

	//inline dds::key_type value() const { return size; }
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
			rec.key = record.value();
			rec.upd = 1;
			rec.ts = record.tstamp();
		}
	}

};


datasrc dds::crawdad_ds(const string& fpath) 
{
	return datasrc(new file_data_source<crawdad_record>(fpath, "r"));
}

datasrc dds::wcup_ds(const string& fpath) 
{
	return datasrc(new file_data_source<wcup_record>(fpath, "r"));
}



std::mt19937 uniform_generator::rng(1961969);


uniform_generator::uniform_generator(stream_id maxsid, source_id maxhid, key_type maxkey)
:	stream_distribution(1,maxsid), source_distribution(1, maxhid),
	key_distribution(1, maxkey), now(0)
{ }

void uniform_generator::set(dds_record& rec) 
{
	rec.sid = stream_distribution(rng);
	rec.hid = source_distribution(rng);
	rec.key = key_distribution(rng);
	rec.upd = 1;
	rec.ts = ++now;
}





uniform_data_source::uniform_data_source(
	stream_id maxsid, source_id maxhid,	key_type maxkey, timestamp maxt
	)
	: gen(maxsid, maxhid, maxkey), maxtime(maxt)
{
	dsm.set_size(maxtime);
	dsm.set_ts_range(1, maxtime);
	dsm.set_key_range(1, maxkey);

	typedef boost::counting_iterator<stream_id> sid_iter;
	dsm.set_stream_range(sid_iter(1), sid_iter(maxsid+1));

	typedef boost::counting_iterator<source_id> hid_iter;
	dsm.set_source_range(hid_iter(1), hid_iter(maxhid+1));

	dsm.set_valid();

	advance();
}

void uniform_data_source::advance()
{
	if(isvalid) {
		if(gen.now < maxtime) {
			gen.set(rec);
		} else {
			isvalid = false;
		}
	}
}





void buffered_dataset::analyze(ds_metadata& meta) const
{
	for(auto& rec : *this) {
		meta.collect(rec);
	}
	meta.set_valid();
}

void buffered_dataset::load(datasrc src) 
{
	for(;src->valid();src->advance())
		this->push_back(src->get());
}

buffered_data_source::buffered_data_source()
: buffer(0)
{ }

void buffered_data_source::set_buffer(buffered_dataset* buf)
{
	buffer = buf;
	buffer->analyze(dsm);
	from = buffer->begin();
	to = buffer->end();
	advance();	
}

buffered_data_source::buffered_data_source(buffered_dataset& dset)
: buffer(&dset), from(dset.begin()), to(dset.end())
{
	buffer->analyze(dsm);
	advance();
}

buffered_data_source::buffered_data_source(buffered_dataset& dset,
	const ds_metadata& meta)
: buffer(&dset), from(dset.begin()), to(dset.end())
{
	dsm = meta;
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


materialized_data_source::materialized_data_source(datasrc src)
{
	dataset.load(src);
	set_buffer(&dataset);
}


/*-----------------------------------------

	HDF5 sources

 -------------------------------------------*/

using namespace H5;
using std::vector;
using binc::print;
using binc::elements_of;


struct hdf5_data_source : data_source
{
	CompType dds_record_type;

	DataSet dset;	
	DataSpace dspc;
	hsize_t total_length;

	buffered_dataset buffer;
	DataSpace mspace;
	hsize_t buffer_size;

	hsize_t curpos;
	buffered_dataset::iterator currec;

	hdf5_data_source(DataSet _dset) : dset(_dset)
	{
		// Check it and load it 
		
		// Create the dds_record type
		dds_record_type = CompType(sizeof(dds_record));
		dds_record_type.insertMember("sid", 
			offsetof(dds_record, sid), H5::PredType::NATIVE_INT16 );
		dds_record_type.insertMember("hid", 
			offsetof(dds_record, hid), H5::PredType::NATIVE_INT16 );
		dds_record_type.insertMember("key", 
			offsetof(dds_record, key), H5::PredType::NATIVE_INT32 );
		dds_record_type.insertMember("upd", 
			offsetof(dds_record, upd), H5::PredType::NATIVE_INT32 );
		dds_record_type.insertMember("ts", 
			offsetof(dds_record, ts), H5::PredType::NATIVE_INT32 );

		//
		//  Start with the metadata
		//
		dspc = dset.getSpace();

		if( dspc.getSimpleExtentNdims()!=1 ) 
			throw std::runtime_error("HDF5 dataset has wrong dimension");
		dspc.getSimpleExtentDims( &total_length );
		dsm.set_size(total_length);

		//
		// Attribute access
		//
		vector<timestamp> ts_range = 
			get_array<timestamp>(dset.openAttribute("ts_range"));
		if(ts_range.size()!=2) {
			print("ts_range size=",ts_range.size(), " contents=", elements_of(ts_range));
			throw std::runtime_error("expected array of size 2 from attribute 'ts_range'");
		}

		vector<key_type> key_range = 
			get_array<timestamp>(dset.openAttribute("key_range"));
		if(key_range.size()!=2)
			throw std::runtime_error("expected array of size 2 from attribute 'key_range'");

		vector<stream_id> sids = 
			get_array<stream_id>(dset.openAttribute("stream_ids"));

		vector<source_id> hids = 
			get_array<source_id>(dset.openAttribute("source_ids"));

		dsm.set_ts_range( ts_range[0], ts_range[1] );
		dsm.set_key_range( key_range[0], key_range[1] );
		dsm.set_stream_range( sids.begin(), sids.end() );
		dsm.set_source_range( hids.begin(), hids.end() );

		dsm.set_valid();

		//
		// Prepare for iteration
		//

 		// 16 Mbytes per read
		resize_buffer(1<<20);

		// position at start of dataset
		curpos = 0;

		advance();
	}

	void resize_buffer(hsize_t bsize)
	{
		// update this for easy access
		buffer_size = bsize;

		// resize the buffer as required.
		// note: the above invalidates currec iterator, so reset it!
		buffer.resize(bsize);
		currec = buffer.end();

		// resize the mspace DataSpace to match new buffer
		mspace.setExtentSimple(1, &bsize);
		mspace.selectAll();
	}

	void fill_buffer()
	{	
		// the file space
		dspc.selectHyperslab(H5S_SELECT_SET, &buffer_size, &curpos);

		// move data
		dset.read(buffer.data(), dds_record_type, mspace, dspc);

		// advance 
		curpos += buffer_size;
	}


	void advance() override 
	{
		if(! isvalid) return;

		if(currec==buffer.end()) 
		{
			// try to read in next slab
			if(curpos == total_length) {
				isvalid = false;
				resize_buffer(0);
				return;
			}

			// let us read the next slab!
			if(total_length-curpos < buffer.size())
				resize_buffer(total_length-curpos);

			fill_buffer();
			currec = buffer.begin();
		}

		rec = *currec;
		++currec;
	}

};


datasrc dds::hdf5_ds(const string& fname, const string& dsetname)
{
	return datasrc( new hdf5_data_source(
		H5File(fname, H5F_ACC_RDONLY).openDataSet(dsetname)
		));	
}


datasrc dds::hdf5_ds(const string& fname)
{
	return hdf5_ds(fname, string("ddstream"));	
}


datasrc dds::hdf5_ds(int locid, const string& dsetname)
{
	return datasrc( new hdf5_data_source(Group(locid).openDataSet(dsetname))  );
}


datasrc dds::hdf5_ds(int dsetid)
{
	return dds::datasrc( new hdf5_data_source(DataSet(dsetid)) );
}




