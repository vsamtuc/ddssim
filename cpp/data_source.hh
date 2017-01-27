#ifndef _DATA_SOURCE_HH_
#define _DATA_SOURCE_HH_

#include <string>
#include <memory>
#include <queue>

#include "dds.hh"

using dds::dds_record;
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

	filtered_data_source(const Func& _func) 
	: sub(new data_source()), func(_func)
	{
		advance();
	}

	const Func& function() const { return func; }

	bool valid() { return isvalid; } 
	void advance() { 
		if(isvalid){
			if(sub->valid()) {
				rec = sub->get();
				isvalid = func(rec);
				sub->advance();
			} else {
				isvalid = false;
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
inline filtered_data_source<Func>* generated_ds(const Func& func)
{
	return new filtered_data_source<Func>(func);
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


/// Setting a record attribute randomly
template <typename AttrType, bool Delta, typename Rng, typename Distr>
struct set_att_func
{
	AttrType dds::dds_record::* attr_ptr;
	AttrType value;
	Rng& rng;
	Distr distr;

	set_att_func(AttrType dds::dds_record::* p, AttrType v, Rng& r, const Distr& d)
	: attr_ptr(p), value(v), rng(r), distr(d) 
	{}

	inline bool operator()(dds::dds_record& rec) {
		if(! Delta) 
			value = distr(rng);
		rec.*attr_ptr = value;
		if(Delta) 
			value += distr(rng);
		return true;
	}
};

template <typename AttrType>
struct set_att_func<AttrType, false, void, void>
{
	AttrType dds::dds_record::* attr_ptr;
	AttrType value;

	set_att_func(AttrType dds::dds_record::* p, AttrType v)
	: attr_ptr(p), value(v)
	{ }

	inline bool operator()(dds::dds_record& rec) {
		rec.*attr_ptr = value;
		return true;
	}		
};

template <typename AttrType>
struct set_att_func<AttrType, true, void, void>
{
	AttrType dds::dds_record::* attr_ptr;
	AttrType value;
	AttrType delta;

	set_att_func(AttrType dds::dds_record::* p, AttrType v, AttrType d)
	: attr_ptr(p), value(v), delta(d)
	{ }

	inline bool operator()(dds::dds_record& rec) {
		rec.*attr_ptr = value;
		value += delta;
		return true;
	}		
};


template <typename T, typename Rng, typename Distr>
auto set_attr(T dds::dds_record::* ptr, Rng& r, const Distr& distr)
{
	return set_att_func<T, false, Rng,Distr>(ptr, (T)0, r, distr);
}

template <typename T>
auto set_attr(T dds::dds_record::* ptr, T val)
{
	return set_att_func<T, false, void, void>(ptr, val);
}


template <typename T, typename Rng, typename Distr>
auto inc_attr(T dds::dds_record::* ptr, T v, Rng& r, const Distr& distr)
{
	return set_att_func<T, true, Rng,Distr>(ptr, v, r, distr);
}

template <typename T>
auto inc_attr(T dds::dds_record::* ptr, T val, T delta)
{
	return set_att_func<T, true, void, void>(ptr, val, delta);
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




#endif
