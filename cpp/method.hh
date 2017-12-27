#ifndef __METHOD_HH___
#define __METHOD_HH___

#include <vector> 
#include <map>
#include <unordered_set>
#include <deque>
#include <list>
#include <utility>

#include <boost/optional.hpp>
#include <jsoncpp/json/json.h>

#include "dds.hh"
#include "eca_event.hh"
#include "data_source.hh"
#include "output.hh"

namespace dds {

using eca::Event;
using eca::eca_rule;
using eca::n_times_out_of_N;

/****************************************

	Factories for on-demand components

 ****************************************/

size_t __hash_hashes(size_t* from, size_t n);

namespace {  
	// use an anonymous namespace for some 
	// template metaprogramming

	// a sequence, e.g.,  seq<0,1,2,2,3>  used to access tuple elements
	// in an unpacking
	template<size_t ...> struct seq { };

	// A compact way to generate a sequence
	// Example:  the inheritance for genseq<3> is
	// genseq<3> : genseq<2,2> : genseq<1, 1, 2> : genseq<0, 0, 1, 2>
	// and genseq<0, 0, 1, 2> defines the type 'type'
	template<size_t N, size_t ...S> struct genseq : genseq<N-1, N-1, S...> { };
	template<size_t ...S>  struct genseq<0, S...> {
	  typedef seq<S...> type;
	};

	// hash for a tuple based on hashes of its elements
	template <typename ...Args, size_t ...S>
	inline size_t __hash_tuple(const std::tuple<Args...>& tuple, seq<S...> _s)
	{
		// unpack hashes of elements in an array (on the stack)
		std::array<size_t, sizeof...(Args)> hashes = {
			(std::hash<Args>()(std::get<S>(tuple)))...
		};
		// return the hash of hashes
		return __hash_hashes(&hashes[0], sizeof...(Args));		
	}

}


class basic_factory {
public:
	virtual void clear()=0;
	virtual ~basic_factory();

};


template <typename T, typename ...Args >
struct factory : basic_factory
{
	static_assert( std::is_same<decltype(T((Args())...)),T>::value ,
		"There does not seem to be a constructor with the given arguments");

	typedef std::tuple<Args...> index_type;

	struct hasher {
		typedef typename genseq<sizeof...(Args)>::type seq_type;
		inline size_t operator()(const index_type& tuple) const {
			return __hash_tuple(tuple, seq_type());
		}
	};

	std::unordered_map<index_type, T*, hasher > registry;

  
	inline T* operator()(Args... args) {
		index_type key(args...);
		auto f = registry.find(key);
		if(f==registry.end()) {
			T* ret = new T(args...);
			registry[key] = ret;
			return ret;
		}
		else
			return f->second;
	}

	void clear() override {
		for(auto m : registry)
			delete m.second;
		registry.clear();		
	}

	~factory() {
		clear();
	}
};


template <typename T>
struct factory<T> : basic_factory
{
	typedef std::tuple<> index_type;
	T* registry;

	inline T* operator()() {
		if(!registry)
			registry = new T();
		return registry;
	}
	factory() : registry(nullptr) {}

	void clear() override {
		if(registry) {
			delete registry;
			registry = nullptr;
		}
	}

	~factory() {
		clear();
	}
};


using fileset_t = std::vector<output_file*>;

class component;
class dataset;
class reporter;


/**
	The CTX context object's type
  */
struct context : eca::engine
{

	// count stream records
	size_t _recno;

	// current time
	timestamp _now;

	// data source
	datasrc ds;

	inline timestamp now() const { return _now; }

	inline const dds_record& stream_record() const { return ds->get(); }

	inline size_t stream_count() const { return _recno; }

	inline const ds_metadata& metadata() const {
		return ds->metadata();
	}

	/**
		Add data source to the controller
	  */
	void data_feed(datasrc src);



	/// Each simulation generates one time series table
	time_series timeseries;
	time_series query_estimate;

	// managed files for results
	fileset_t result_files;

	output_file* open(FILE* f, bool owner, text_format fmt);
	output_file* open(const string& path, open_mode mode, text_format fmt);
	output_file* open_hdf5(const string& path, 
		open_mode mode = default_open_mode);

	buffered_dataset warmup;	

	void close_result_files();
	void clear();

	/// Must be default-constructible!
	context();

	/// initialized to a unique id
	string run_id;

	/// Initialize the context for a new execution
	void initialize();

	/// Start the simulation
	void run();
};


///  The global context
extern context CTX;



/// Used to add ECA rules
template <typename Action>
inline auto ON(Event evt, const Action& action)
{
	return CTX.add_rule(evt, new eca::action_function<Action>(action));
}

template <typename Condition, typename Action>
inline auto ON(Event evt, const Condition& cond, const Action& action)
{
	return CTX.add_rule(evt, 
		new eca::condition_action<Condition, Action>(cond, action));
}

/// Used to emit an event
inline void emit(Event evt)
{
	CTX.emit(evt);
}


inline n_times_out_of_N n_times(size_t n)
{
	return n_times_out_of_N(n, CTX.metadata().size());
}




class component;


/**
	A component type is a factry for components of a certain type.

	This is the abstract base class. Subclasses need to implement the
	abstract method \c create(json).

	Each component type has a unique name. The json config file can 
	declare such components by the 'type' attribute.
  */
class basic_component_type : public named
{
protected:
	static std::map<string, basic_component_type*>& ctype_map();
	basic_component_type(const string& _name);
	basic_component_type(const type_info& ti);
	virtual ~basic_component_type();
public:
	virtual component* create(const Json::Value&) = 0;

	static basic_component_type* get_component_type(const string& _name);
	static basic_component_type* get_component_type(const type_info& ti);
	static const std::map<string, basic_component_type*>& component_types();
	static std::set<string> aliases(basic_component_type* ctype);
};

template <typename C>
class component_type : public basic_component_type
{
public:
	component_type(const string& _name) : basic_component_type(_name) {}
	component_type() : basic_component_type(typeid(C)) {}
	virtual C* create(const Json::Value&) override ;
};


class component : public virtual named, public eca::reactive
{
public:
	component();
	component(const string& _name);
	virtual ~component();
};




/**
	Used to prepare and load a dataset to the
	context
  */
class dataset : public component
{
	datasrc base_src;
	datasrc src;

	boost::optional<string> _name;

	
	boost::optional<size_t> _loops;
	boost::optional<size_t> _max_length;
	boost::optional<timestamp> _max_timestamp;
	boost::optional<stream_id> _streams;
	boost::optional<source_id> _sources;
	boost::optional<timestamp> _time_window;
	boost::optional<size_t> _fixed_window;
	bool _wflush;

	boost::optional<size_t> _warmup_size;
	boost::optional<timestamp> _warmup_time;

	datasrc apply_filters();
	void create_no_warmup();
	void create_warmup_size(size_t wsize);
	void create_warmup_time(timestamp wtime);
	ds_metadata collect_metadata();
public:
	dataset();
	~dataset();

	void clear();
	void load(datasrc _src);

	void set_name(const string&);
	void set_loops(size_t loops);
	void set_max_length(size_t n);
	void set_max_timestamp(timestamp t);
	void hash_streams(stream_id h);
	void hash_sources(source_id s);
	void set_time_window(timestamp Tw, bool flush);
	void set_fixed_window(size_t W, bool flush);

	void warmup_size(size_t wsize);
	void warmup_time(timestamp wtime);

	void create();
};


/**
	A component that manages output.

	An instance of this class can perform actions during
	a simulation, such as
	- enable/disable output tables and/or bindings
	- call prolog() and epilog() on output tables
	- call close() on output files
	- call emit_row() on timeseries using a particular
	  sampling logic 
  */
class reporter : public component
{
	// remember output tables we watch
	std::unordered_set<output_table*> _watched;
public:

	/**
		Call `prolog()` and `epilog()` on the table.

		The events on which the `prolog()` and `epilog()` methods
		are called depend on the table flavor:
		- For `result_table` tables, `INIT` and `DONE`.
		- For `time_series` tables, `START_STREAM` and `END_STREAM`.

		Calling this method multiple times for the same table is
		acceptable.
	  */
	void watch(output_table& otab)
	{
		if(_watched.find(&otab) == _watched.end()) {
			switch(otab.flavor()) {
				case table_flavor::RESULTS:
					on(INIT, [&]() { otab.prolog(); });
					on(DONE, [&]() { otab.epilog(); });
					break;
				case table_flavor::TIMESERIES:
					on(START_STREAM, [&]() { otab.prolog(); });
					on(END_STREAM, [&]() { otab.epilog(); });
					break;				
			}
			_watched.insert(&otab);
		}
	}

	/**
		Registers a conditional action to emit a `time_series` row.

		The method registers an ECA rule of the form
		```
		  on(REPORT, emit_cond, emit_action)
		```
		Also, the `ts` object becomes wathced.

		The `emit_cond` can determine some sampling strategy, e.g.,
		using `struct every_n_times` or `struct n_times_out_of_N`
		arguments. For example, to emit time-series of 100 elements
		over the run (assuming that the `data_feed` of the context has
		been initialized), one can call
		```
		rep.emit_row(ts, n_times_out_of_N(100, CTX.metadata().size()));
		```

		@see watch
		@see struct n_times_out_of_N
		@see struct every_n_times
		@see struct n_times
	  */
	template <typename Cond>
	void emit_row(time_series& ts, const Cond& emit_cond)
	{
		watch(ts);
		on(REPORT, emit_cond , [&]() { 
			ts.emit_row();
		});
	}

	/**
		Set the sampling size for a time_series.

		The sample will include the initial and final record
		and will be linearly spread through the stream size.

		This method is a shortcut for
		```
		emit_row(ts, n_times(nsamp));
		```
	  */
	void sample(time_series& ts, size_t nsamp)
	{
		emit_row(ts, n_times(nsamp));
	}

};


struct progress_reporter : public component, progress_bar
{
	progress_reporter(size_t _marks) 
	: progress_reporter(stdout,_marks,"Progress:") {}

	progress_reporter(FILE* _stream=stdout, 
		size_t _marks = 40, 
		const string& _msg = "") 
	: progress_bar(_stream, _marks, _msg)
	{
		on(START_STREAM, [&](){ start(CTX.metadata().size()); });
		on(START_RECORD, [&](){ tick(); });
		on(END_STREAM, [&](){ finish(); });
	}
};




} //end namespace dds

#endif
