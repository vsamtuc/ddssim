#ifndef _DATA_SOURCE_HH_
#define _DATA_SOURCE_HH_

#include <string>
#include <memory>
#include <queue>
#include <vector>
#include <type_traits>

#include "dds.hh"


namespace dds {

using std::shared_ptr;

/**
	A data source is an object providing the data of a stream.
	The API is very similar to an iterator.
  */
class data_source
{
protected:
	bool isvalid;
	dds::dds_record rec;
public:

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

/// A convenient name
using dsref = shared_ptr<data_source>;


/**
	Generate a record stream by applying a function
	on the records of another stream.
  */
template <typename Func>
class filtered_data_source : public data_source
{
protected:
	dsref sub;
	Func func;
public:
	filtered_data_source(data_source* _sub, const Func& _func) 
	: sub(_sub), func(_func)
	{
		advance();
	}

	filtered_data_source(const dds::dds_record& initrec, const Func& _func) 
	: data_source(initrec), sub(0), func(_func)
	{
		advance();
	}

	const Func& function() const { return func; }

	bool valid() { return isvalid; } 
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
	const dds_record& get_record() { return rec; }
};

/// Construct a generated data source
template <typename Func>
inline filtered_data_source<Func>* filtered_ds(data_source* ds, const Func& func)
{
	return new filtered_data_source<Func>(ds, func);
}

/// Construct a generated data source
template <typename Func>
inline filtered_data_source<Func>* generated_ds(const dds::dds_record& rec, const Func& func)
{
	return new filtered_data_source<Func>(rec, func);
}


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


/// Construct a function sequence
template <typename Func1, typename Func2>
auto operator|(const Func1& f1, const Func2& f2) {
	return function_sequence<Func1,Func2>(f1,f2);
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

	dsref sub;
	dds::timestamp Tw;
	Window window;

public:	

	time_window_source(data_source* _sub, dds::timestamp _w);

	inline auto delay() const { return Tw; }

	void advance();
};


inline auto time_window(data_source* ds, dds::timestamp Tw)
{
	return new time_window_source(ds, Tw);
}




/**
	Data source factory functions for file formats
  */

data_source* crawdad_ds(const std::string& fpath);
data_source* wcup_ds(const std::string& fpath);


/**
	A main-memory store of stream records.
 */
class buffered_dataset : public std::vector<dds::dds_record>
{
public:
	using std::vector<dds::dds_record>::vector;

};




/**
	Buffered data source.

	This is a data source that reads a given data source into
	memory (TODO: spillover to disk), collects metadata, and
	then replays the data.

	(TODO: make the data source can be rewind multiple times,
	 if to create a long stream)
  */
class buffered_data_source : public data_source
{
	dds::ds_metadata dsm;
	buffered_dataset buffer;
	typedef std::vector<dds_record>::iterator bufiter;

	bufiter from, to;

	void collect_metadata(data_source* ds);

public:
	buffered_data_source(data_source* inputds, size_t size_hint=1024);

	inline const dds::ds_metadata& metadata() const { return dsm; }

	void advance() override;
};


}


#endif
