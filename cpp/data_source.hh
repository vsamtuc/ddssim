#ifndef _DATA_SOURCE_HH_
#define _DATA_SOURCE_HH_

#include <string>
#include <deque>
#include <list>
#include <vector>
#include <map>
#include <type_traits>
#include <random>
#include <algorithm>

#include <boost/shared_ptr.hpp>
#include <boost/iterator/counting_iterator.hpp>

#include "dds.hh"


namespace dds {

using boost::shared_ptr;
using boost::dynamic_pointer_cast;
using boost::static_pointer_cast;


class data_source;

/**
	Shared pointed to data source objects.

  */
typedef shared_ptr<data_source> datasrc;


/**
	A main-memory store of stream records.
	
	TODO: spillover to disk
 */
class buffered_dataset : public std::vector<dds::dds_record>
{
public:
	using std::vector<dds::dds_record>::vector;

	/// Return a metadata object for the buffered data
	void analyze(ds_metadata &) const;

	/// Load all data from a data source
	void load(datasrc src);

};




/**
	A data source is an object providing the data of a stream.
	The API is very similar to an iterator. 
	
	Data sources should only be held by `std::shared_ptr` 
	inside other objects or in functions.
  */
class data_source : public std::enable_shared_from_this<data_source>
{
protected:
	ds_metadata dsm;
	bool isvalid;
	dds_record rec;
	friend struct iterator;

public:

	struct iterator 
		: std::iterator<std::input_iterator_tag,
			const dds_record>
	{
	private:
		data_source* src;
		inline data_source* __eff() const { 
			return (src && src->isvalid)?src:nullptr; 
		}
	public:
		iterator() : src(nullptr) {}
		iterator(data_source* _src) : src(_src) {}

		iterator& operator++() { src->advance(); return *this; }
		iterator& operator++(int) { src->advance(); return *this; }
		reference operator*() const { return src->rec; }
		bool operator==(iterator other) const { 
			return this->__eff() == other.__eff();
		}
		bool operator!=(iterator other) const { return !(*this == other); }
	};

	/**
		Return an input iterator to the (current) beginning of the source.
	  */
	inline iterator begin() { return iterator(this); }

	/**
		Return an input iterator to the end of the source.
	  */
	inline iterator end() { return iterator(nullptr); }

	inline data_source() : isvalid(true) {}

	inline data_source(const dds::dds_record& _rec) 
		: isvalid(true), rec(_rec) {}

	/**
		When this method returns true, the method \c get()
		returns a valid dds_record.
	  */
	inline bool valid() const { return isvalid; }
	inline operator bool() const { return valid(); }

	/**
		Return the current valid record or throw an expection.
	  */
	inline const dds_record& get() const { return rec; }

	/**
		Return the current valid record or throw an expection.
	  */
	inline const dds_record& operator*() const { return get(); }

	/**
		Advance the data source to the next record
	  */
	virtual void advance() {}


	/**
		Advance the data source and return next record

		This is a _dangerous_ operation, since advancing the
		data source may invalidate the source.
	  */
	inline const dds_record& operator++() { advance(); return get(); }

	/**
		Return the current record and then update the data source.

	  */
	inline dds_record operator++(int) { 
		dds_record ret = get();
		advance(); 
		return ret; 
	}

	/**
		Check if this data source is rewindable.
	  */
	virtual bool rewindable() const { return false; }
	virtual void rewind() { throw std::runtime_error("Data source is not rewindable"); }

	//------------------------------------
	//
	//  Warmup creation
	//
	//------------------------------------

	virtual void warmup_time(timestamp wtime, buffered_dataset* buf);
	virtual void warmup_size(size_t wsize, buffered_dataset* buf);

	/// The metadata for this source
	inline const ds_metadata& metadata() const { return dsm; }
	inline void set_name(const std::string& _name) { dsm.set_name(_name); }
	inline void set_warmup_time(timestamp tw) { dsm.set_warmup_time(tw); }
	inline void set_warmup_size(size_t sw) { dsm.set_warmup_size(sw); }
	inline void set_metadata(const ds_metadata& other) { dsm = other; }
	inline bool analyzed() const {  return dsm.valid(); }

	/// Virtual destructor
	virtual ~data_source() { }
};


/**
	Rewindable data source.

	A data source that can be reset so that its stream can be replayed.	
  */
class rewindable_data_source : public data_source
{
public:

	virtual bool rewindable() const override { return true; }

};


/**
	Create a data source object passing properties.

	This function can be used to create data source objects in a generic way.
	The arguments rougly correspond to a URI of the form

	\c type:name?opt1=val1,...,optn=valn

	@param type designate a particular type of data source, e.g. a data format
	@param name designate an instance of this type of data source, e.g. a file name
	@param options a string->string map of options e.g., ways to interpret the file's data

	@throws std::invalid_argument if the type is unknown
  */
datasrc open_data_source(const std::string& type,
		const std::string& name,
		const std::map<std::string, std::string>& options = std::map<std::string,std::string>());


//------------------------------------
//
//  Looped data source
//
//------------------------------------

/**
	Replay a given data source a number of times, adjusting the timestamp.
  */
class looped_data_source : public rewindable_data_source
{
protected:
	datasrc sub;
	size_t loops;
	size_t loop;
	timestamp toffset, tlast;
public:
	looped_data_source(datasrc _sub, size_t _loops);

	void rewind() override;
	void advance() override;	
};

inline datasrc looped_ds(datasrc _sub, size_t nloops)
{
	return datasrc(new looped_data_source(_sub, nloops));
}


//------------------------------------
//
//  Filtered and generated data sources
//
//------------------------------------



/**
	Generate a record stream by applying a function
	on the records of another stream.
  */
template <typename Func>
class filtered_data_source : public data_source
{
protected:
	datasrc sub;
	Func func;
public:
	filtered_data_source(datasrc _sub, const Func& _func) 
	: sub(_sub), func(_func)
	{
		set_metadata(sub->metadata());
		func(dsm);

		advance();
	}

	const Func& function() const { return func; }

	bool rewindable() const override
	{
		return func.rewindable() && sub->rewindable();
	}

	void rewind() override
	{
		func.rewind();
		sub->rewind();
		isvalid = true;
		advance();
	}

	void advance() override
	{ 
		if(isvalid){
		 	if(sub->valid()) {
				rec = sub->get();
				isvalid = func(rec);
				sub->advance();
			} else  {
				isvalid = false;
			} 
		}
	}
};

/// Construct a generated data source
template <typename Func>
inline auto filtered_ds(datasrc ds, const Func& func)
{
	return datasrc(new filtered_data_source<Func>(ds, func));
}



//------------------------------------
//
//  Helpers for filtering functions
//
//------------------------------------



/// A maximum_length filter
struct max_length 
{
	size_t count = 0;
	size_t N;
	max_length(size_t n) : N(n) {}
	inline bool operator()(dds::dds_record&) {
		if(count<N) {
			count++; 
			return true;
		} 
		else 
			return false;
	}

	void operator()(ds_metadata& dsm) {
		// We cannot know the new end-time
		dsm.set_valid(false);
	}

	bool rewindable() const { return true; }
	void rewind() { count=0; }
};


/// A maximum-time  filter
struct max_timestamp
{
	timestamp tend;

	max_timestamp(timestamp _tend) : tend(_tend) {}

	inline bool operator()(dds::dds_record& rec) {
		return rec.ts <= tend;
	}

	void operator()(ds_metadata& dsm) {
		// We cannot know the new size
		dsm.set_valid(false);
	}

	bool rewindable() const { return true; }
	void rewind() { }
};



/// A filter to hash source id or stream id by applying modulo
struct modulo_attr
{
	typedef int16_t intid;
	intid dds_record::* ptr;
	intid n;

	modulo_attr(intid dds_record::* _ptr, intid _n) 
	: ptr(_ptr), n(_n)
	{ }

	inline bool operator()(dds_record& rec) { 
		rec.*ptr %= n;
		return true;
	}

	// this is an ugly hack!!!
	void operator()(ds_metadata& dsm) {
		bool onsid = ptr == &dds_record::sid;

		if(! onsid && ptr != &dds_record::hid)
			throw std::logic_error("unknown dds_record attribute in modulo_attr_func");

		set<intid> ids {  (onsid)? dsm.stream_ids() : dsm.source_ids()  };
		set<intid> newids;

		for(auto id : ids) newids.insert( id % n );

		if(onsid)
			dsm.set_stream_range(newids.begin(), newids.end());
		else
			dsm.set_source_range(newids.begin(), newids.end());
	}

	bool rewindable() const { return true; }
	void rewind() { }
};




//------------------------------------
//
//  Sliding Windows
//
//------------------------------------


/**
	\brief A time-based sliding window

	A time window is a sliding window filter that removes
	records after an expiration interval Tw.
  */
class time_window_source : public data_source
{
	void advance_from_sub();
	void advance_from_window();
protected:
	typedef std::deque<dds::dds_record> Window;

	datasrc sub;
	dds::timestamp Tw;
	Window window;
	bool flush;

public:	

	time_window_source(datasrc _sub, timestamp _w, bool _flush);
	inline auto delay() const { return Tw; }
	bool flush_window() const { return flush; }
	void advance();

	bool rewindable() const override { return sub->rewindable(); }
	void rewind() override;
};


inline datasrc time_window(datasrc ds, timestamp Tw, bool flush)
{
	return datasrc(new time_window_source(ds, Tw, flush));
}



/**
	\brief A fixed-size sliding window

	A fixed window is a sliding window filter that removes
	records after seeing W additional records forward.
  */
class fixed_window_source : public data_source
{
	void advance_from_sub();
	void advance_from_window();
protected:
	typedef std::deque<dds::dds_record> Window;

	datasrc sub;
	size_t W;
	Window window;
	timestamp tflush;
	bool flush;

public:	

	fixed_window_source(datasrc _sub, size_t W, bool _flush);
	inline auto window_size() const { return W; }
	bool flush_window() const { return flush; }
	void advance();

	bool rewindable() const override { return sub->rewindable(); }
	void rewind() override;
};


inline datasrc fixed_window(datasrc ds, size_t W, bool flush)
{
	return datasrc(new fixed_window_source(ds, W, flush));
}





//------------------------------------
//
//  Data files
//
//------------------------------------


/**
	Data source factory function for the Crawdad file format.

	This call is equivalent to 
	\c open_data_source("crawdad", fpath)
  */
datasrc crawdad_ds(const std::string& fpath);

/**
	Data source factory function for the WorldCup file format.

	This call is equivalent to 
	\c open_data_source("wcup", fpath)
  */
datasrc wcup_ds(const std::string& fpath);


/**
   Load the dataset found in an HDF5 file with the given name.

   This call is equivalent to 
   \c open_data_source("hdf5", fname, {"dataset", dsetname})
  */
datasrc hdf5_ds(const std::string& fname, const std::string& dsetname);


/**
   Load the dataset found in an HDF5 file with the given name.

   This call is equivalent to 
   \c open_data_source("hdf5", fname, {"dataset", "ddstream"})
  */
datasrc hdf5_ds(const std::string& fname /* dsetname=="ddstream" */);

/**
   Load the dataset found at the location locid of an HDF5 file,
   with the given name.
 */
datasrc hdf5_ds(int locid, const std::string& dsetname);

/**
   Load the dataset with the given dataset id.
 */
datasrc hdf5_ds(int dsetid);




//------------------------------------
//
//  Synthetic data sources
//
//------------------------------------



/**
	Generates a stream of random stream records
  */
struct uniform_generator
{
private:
	static std::mt19937 seed_rng;
	std::mt19937 rng;
	unsigned int seed;
public:

	template <typename T>
	using uni = std::uniform_int_distribution<T>;

	uni<stream_id> stream_distribution;
	uni<source_id> source_distribution;
	uni<key_type> key_distribution;
	//timestamp maxtime;
	timestamp now;

	uniform_generator(unsigned seed, stream_id maxsid, source_id maxhid, key_type maxkey);
	uniform_generator(stream_id maxsid, source_id maxhid, key_type maxkey);

	void set(dds_record& rec);

	inline dds_record operator()() {
		dds_record ret;
		set(ret);
		return ret;
	}

	void reinitialize();
};

/**
	A data source from a uniform generator
  */
struct uniform_data_source : rewindable_data_source
{
	uniform_generator gen;
	timestamp maxtime;

	uniform_data_source(stream_id maxsid, source_id maxhid,
		 key_type maxkey, timestamp maxt);

	void advance() override ;
	void rewind() override;

};


inline datasrc uniform_datasrc(stream_id maxsid, source_id maxhid,
		 key_type maxkey, timestamp maxt)
{
	return datasrc(new uniform_data_source(maxsid, maxhid, maxkey, maxt));
}


//------------------------------------
//
//  Data buffering in memory
//
//------------------------------------



/**
	Create a uniform dataset with filtering
  */
template <typename F>
inline buffered_dataset 
make_uniform_dataset(stream_id maxsid, source_id maxhid,
 key_type maxkey, timestamp maxts, const F& _func)
{
	datasrc ds = new uniform_data_source(maxsid, maxhid, maxkey, maxts);
	datasrc fds = filtered_ds(ds, _func);
	buffered_dataset dset;
	dset.load(fds);
	return dset;
}

/**
	Create a uniform dataset without filtering.
  */

inline buffered_dataset 
make_uniform_dataset(stream_id maxsid, source_id maxhid,
 key_type maxkey, timestamp maxts)
{
	auto ds = datasrc(new uniform_data_source(maxsid, maxhid, maxkey, maxts));
	buffered_dataset dset;
	dset.load(ds);
	return dset;
}



/**
	Buffered data source.

	This is a data source that, given a dataset,
	optionally collects metadata, and then replays the data.

	TODO: make the data source rewindable multiple times,
	to create a long stream
  */
class buffered_data_source : public rewindable_data_source
{
	buffered_dataset* buffer;

	typedef buffered_dataset::iterator bufiter;
	bufiter from, to;
protected:
	buffered_data_source();
	void set_buffer(buffered_dataset*);
public:
	/// Make a data source from a dataset
	buffered_data_source(buffered_dataset& dset);

	/// Make a data source from a dataset, use given metadata
	buffered_data_source(buffered_dataset& dset, const ds_metadata& meta);

	/// The metadata for this source
	inline buffered_dataset& dataset() const { return *buffer; }

	void rewind() override;

	void advance() override;
};


/**
	A buffered data source which includes the dataset internally. 
  */
class materialized_data_source : public buffered_data_source
{
protected:
	buffered_dataset dataset;
public:
	materialized_data_source(datasrc src);

};

inline datasrc materialize(datasrc src)
{
	return datasrc(new materialized_data_source(src));
}




struct cascade_data_source : data_source
{
protected:
	std::list<datasrc> sources;
	void init();
public:
	cascade_data_source(std::initializer_list<datasrc> src);

	template<typename Iter>
	cascade_data_source(Iter iter1, Iter iter2) 
	: sources(iter1, iter2)
	{
		init();
	}

	void advance();
};



};


#endif
