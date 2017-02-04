#ifndef _DATA_SOURCE_HH_
#define _DATA_SOURCE_HH_

#include <string>
#include <memory>
#include <queue>
#include <vector>
#include <type_traits>
#include <random>

#include <boost/iterator/counting_iterator.hpp>

#include "dds.hh"


namespace dds {


class data_source;


/**
	A data source is an object providing the data of a stream.
	The API is very similar to an iterator.
  */
class data_source
{
protected:
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

		iterator& operator++(int) { src->advance(); return *this; }
		reference operator*() const { return src->rec; }
		bool operator==(iterator other) const { 
			return this->__eff() == other.__eff();
		}
		bool operator!=(iterator other) const { return !(*this == other); }
	};

	inline iterator begin() { return iterator(this); }
	inline iterator end() { return iterator(nullptr); }

	inline data_source() : isvalid(true) {}

	inline data_source(const dds::dds_record& _rec) 
		: isvalid(true), rec(_rec) {}

	/**
		When this method returns true, the method \c get_record()
		returns the next valid dds_record.
	  */
	inline bool valid() const { return isvalid; }
	inline operator bool() const { return valid(); }

	/**
		Return the current valid record or throw an expection.
	  */
	inline const dds_record& get() const { return rec; }
	inline const dds_record& operator*() const { return get(); }

	/**
		Advance the data source to the next record
	  */
	virtual void advance() {}
	inline const dds_record& operator++() { advance(); return get(); }
	inline dds_record operator++(int) { 
		dds_record ret = get();
		advance(); 
		return ret; 
	}

	/// Virtual destructor
	virtual ~data_source() { }
};


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
	std::unique_ptr<data_source> sub;
	Func func;
public:
	filtered_data_source(data_source* _sub, const Func& _func) 
	: sub(_sub), func(_func)
	{
		advance();
	}

	filtered_data_source(const dds::dds_record& initrec, const Func& _func) 
	: data_source(initrec), sub(), func(_func)
	{
		advance();
	}

	const Func& function() const { return func; }

	void advance() { 
		if(isvalid){
			if(sub && sub->valid()) {
				rec = sub->get();
				isvalid = func(rec);
				sub->advance();
			} else if(sub) {
				isvalid = false;
			} else {
				isvalid = func(rec);
			}
		}
	}
};

/// Construct a generated data source
template <typename Func>
inline auto filtered_ds(data_source* ds, const Func& func)
{
	return new filtered_data_source<Func>(ds, func);
}

/// Construct a generated data source
template <typename Func>
inline auto generated_ds(const dds_record& rec, const Func& func)
{
	return new filtered_data_source<Func>(rec, func);
}


//------------------------------------
//
//  Helpers for filtering functions
//
//------------------------------------



/**
	A functional that calls one functional after the other.
  */
template <typename F1, typename F2>
struct function_sequence
{
	F1 f1;
	F2 f2;
	function_sequence(const F1& _f1, const F2& _f2) : f1(_f1), f2(_f2) {}
	inline bool operator()(dds::dds_record& rec) {
		return f1(rec) && f2(rec);
	}
};

template <>
struct function_sequence<void, void> 
{
	inline bool operator()(dds::dds_record& rec) const { return true; }	
};

const function_sequence<void, void>  FSEQ;

/// Construct a function sequence
template <typename F1, typename F2, typename F>
auto operator|(const function_sequence<F1,F2>& fs, const F& f) {
	typedef function_sequence<F1,F2> FS;
	return function_sequence<FS,F>(fs,f);
}


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
};


/*
	Setting or incrementing attributes
 */

template <typename AttrType, typename Func>
struct set_attr_f
{
	AttrType dds::dds_record::* attr;
	Func func;
	
	inline set_attr_f(AttrType dds::dds_record::* _attr, const Func& f)
	: attr(_attr), func(f) {}
	
	inline bool operator()(dds::dds_record& rec) {
		rec.*attr = func(rec);
		return true;
	}	
};


template <typename T, typename Rng, typename Distr>
auto set_attr(T dds::dds_record::* ptr, Rng& r, Distr& distr)
{
	auto f = [&r, &distr](const dds::dds_record& rec) -> T {
		return distr(r);
	};

	return set_attr_f<T, decltype(f)>(ptr, f);
}

template <typename T>
auto set_attr(T dds::dds_record::* ptr, T val)
{
	//return set_att_func<T, false, void, void>(ptr, val);
	auto f = [val](const dds::dds_record& rec) {
		return val;
	};
	return  set_attr_f<T, decltype(f)>(ptr, f);
}


template <typename T, typename Rng, typename Distr>
auto addto_attr(T dds::dds_record::* ptr, Rng& r, Distr& distr)
{
	//distribution<Rng, Distr> d(r, distr);
	auto dfunc = [ptr, &r, &distr](const dds::dds_record& rec) {
			return rec.*ptr + distr(r);
	};
	return set_attr_f<T, decltype(dfunc)>(ptr, dfunc);
}

template <typename T>
auto addto_attr(T dds::dds_record::* ptr, T delta)
{
	//return set_att_func<T, true, void, void>(ptr, val, delta);
	auto dfunc = [ptr, delta](const dds::dds_record& rec) {
			return rec.*ptr + delta;
		};
	return  set_attr_f<T, decltype(dfunc)>(ptr, dfunc);
}

template <typename T>
auto modulo_attr(T dds::dds_record::* ptr, T n)
{
	auto dfunc = [ptr, n](const dds::dds_record& rec) {
			return rec.*ptr % n;
		};
	return  set_attr_f<T, decltype(dfunc)>(ptr, dfunc);
}


//------------------------------------
//
//  Sliding Windows
//
//------------------------------------


/**
	A time window is a window filter that removes
	records after an expiration interval Tw.
  */
class time_window_source : public data_source
{
	void advance_from_sub();
	void advance_from_window();
protected:
	typedef std::queue<dds::dds_record> Window;

	std::unique_ptr<data_source> sub;
	dds::timestamp Tw;
	Window window;

public:	

	time_window_source(data_source* _sub, timestamp _w);
	inline auto delay() const { return Tw; }
	void advance();
};


inline auto time_window(data_source* ds, timestamp Tw)
{
	return new time_window_source(ds, Tw);
}


//------------------------------------
//
//  Data files
//
//------------------------------------



/**
	Data source factory functions for file formats
  */

data_source* crawdad_ds(const std::string& fpath);
data_source* wcup_ds(const std::string& fpath);


//------------------------------------
//
//  Synthetic data sources
//
//------------------------------------

/**
	Base class for a data source with metadata.
  */
class analyzed_data_source : public data_source
{
protected:
	ds_metadata dsm;
public:
	/// The metadata for this source
	inline const ds_metadata& metadata() const { return dsm; }
};


/**
	Generates a stream of random stream records
  */
struct uniform_generator
{
private:
	static std::mt19937 rng;
public:

	template <typename T>
	using uni = std::uniform_int_distribution<T>;

	uni<stream_id> stream_distribution;
	uni<source_id> source_distribution;
	uni<key_type> key_distribution;
	timestamp maxtime;
	timestamp now;

	uniform_generator(stream_id maxsid, 
			source_id maxhid, 
			key_type maxkey):
		stream_distribution(1,maxsid), source_distribution(1, maxhid),
		key_distribution(1, maxkey), now(0)
	{ }

	void set(dds_record& rec) {
		rec.sid = stream_distribution(rng);
		rec.hid = source_distribution(rng);
		rec.sop = INSERT;
		rec.ts = ++now;
		rec.key = key_distribution(rng);
	}

	inline dds_record operator()() {
		dds_record ret;
		set(ret);
		return ret;
	}
};

struct uniform_data_source : analyzed_data_source
{
	uniform_generator gen;
	timestamp maxtime;

	uniform_data_source(stream_id maxsid, source_id maxhid,
		 key_type maxkey, timestamp maxt)
	: gen(maxsid, maxhid, maxkey), maxtime(maxt)
	{
		dsm.set_size(maxtime);
		dsm.set_ts_range(1, maxtime);
		dsm.set_key_range(1, maxkey);

		typedef boost::counting_iterator<stream_id> sid_iter;
		dsm.set_stream_range(sid_iter(1), sid_iter(maxsid+1));

		typedef boost::counting_iterator<source_id> hid_iter;
		dsm.set_source_range(hid_iter(1), hid_iter(maxhid+1));
		advance();
	}

	void advance() override 
	{
		if(isvalid) {
			if(gen.now < maxtime) {
				gen.set(rec);
			} else {
				isvalid = false;
			}
		}
	}
};




//------------------------------------
//
//  Data buffering in memory
//
//------------------------------------



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
	void load(data_source* src);

	/// Load all data from a data source and dispose of it
	inline void consume(data_source* src) 
	{
		load(src);
		delete src;
	}
};




/**
	Buffered data source.

	This is a data source that, given a dataset,
	optionally collects metadata, and then replays the data.

	TODO: make the data source rewindable multiple times,
	to create a long stream
  */
class buffered_data_source : public analyzed_data_source
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
	materialized_data_source(data_source* src);

};


};


#endif
