#ifndef __OUTPUT_HH__
#define __OUTPUT_HH__

#include <string>
#include <vector>
#include <cstdio>
#include <cstring>
#include <list>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <algorithm>
#include <typeinfo>
#include <typeindex>

#include "dds.hh"

namespace H5
{
	struct Group;
	struct H5File;
}

namespace dds
{

using std::string;
using std::type_index;
using std::type_info;

/*-----------------------------
	Generic classes
  -----------------------------*/

class output_table;

class basic_column : public named
{
protected:
	output_table* _table;
	size_t _index;
	string _format;
	type_index _type;
	size_t _size;
	size_t _align;

	friend class output_table;
public:
	basic_column(
		output_table* _tab, const string& _name, const string& f, 
		const type_info& _t, size_t _s, size_t _a);

	basic_column(const basic_column&)=delete;
	basic_column(const basic_column&&)=delete;
	virtual ~basic_column();

	inline output_table* table() const { return _table; }
	inline size_t index() const { return _index; }

	inline const char* format() const { return _format.c_str(); }
	virtual void emit(FILE*) = 0;
	virtual void copy(void*) = 0;

	virtual bool is_arithmetic() const { return false; }
	inline type_index type() const { return _type; }
	inline size_t size() const { return _size; }
	inline size_t align() const { return _align; }

	// Using these, one can set a basic_column if only it is known
	// whether it is arithmetic or not.
	// The default implementation throws invalid_argument
	virtual void set(double val);
	virtual void set(const string&);
};



template <typename T>
class column : public basic_column
{
	static_assert(std::is_arithmetic<T>::value, 
		"Column on non-arithmentic type");
protected:
	T val;
public:
	column(output_table* _tab, const string& _n, const string& fmt) 
	: basic_column(_tab, _n, fmt, typeid(T), sizeof(T), alignof(T)) { }

	column(const string& _n, const string& fmt) 
	: basic_column(nullptr, _n, fmt, typeid(T), sizeof(T), alignof(T)) { }

	column(output_table* _tab, const string& _n, const string& fmt, const T& _v) 
	: basic_column(_tab, _n, fmt, typeid(T), sizeof(T), alignof(T)), val(_v) { }

	column(const string& _n, const string& fmt, const T& _v) 
	: basic_column(nullptr, _n, fmt, typeid(T), sizeof(T), alignof(T)), val(_v) { }

	inline T value() const { return val; }
	inline T& value() { return val; }
	inline void set_value(const T& v) { val=v; }
	inline T& operator=(T v) { val=v; return val; }
	void emit(FILE* s) override {
		fprintf(s, format(), value());
	}
	void copy(void* ptr) { memcpy(ptr, &val, _size); }

	void set(double val) override
	{
		set_value(val);
	}
	bool is_arithmetic() const override { 
		return std::is_arithmetic<T>::value; 
	}

};

template <>
class column<string> : public basic_column
{
protected:
	const size_t maxlen;
	string val;
public:
	column(output_table* _tab, const string& _n, size_t _maxlen, const string& fmt) 
	: basic_column(_tab, _n, fmt, 
		typeid(string), sizeof(char[_maxlen+1]), alignof(char[_maxlen+1])), 
		maxlen(_maxlen) 
		{ }

	column(const string& _n, size_t _maxlen, const string& fmt) 
	: basic_column(nullptr, _n, fmt, 
		typeid(string), sizeof(char[_maxlen+1]), alignof(char[_maxlen+1])), 
		maxlen(_maxlen) 
		{ }

	column(output_table* _tab, const string& _n,  size_t _maxlen, const string& fmt, const string& _v) 
	: basic_column(_tab, _n, fmt, 
		typeid(string), sizeof(char[_maxlen+1]), alignof(char[_maxlen+1])), 
			maxlen(_maxlen), val(_v) 
		{ }

	column(const string& _n,  size_t _maxlen, const string& fmt, const string& _v) 
	: basic_column(nullptr, _n, fmt, 
		typeid(string), sizeof(char[_maxlen+1]), alignof(char[_maxlen+1])), 
			maxlen(_maxlen), val(_v) 
		{ }

	inline const string& value() const { return this->val; }
	inline string& value() { return val; }
	inline void set_value(const string& v) { 
		val=v.substr(0,maxlen); 
	}
	inline string& operator=(const string& v) { 
		set_value(v);
		return val; 
	}
	void emit(FILE* s) override {
		fprintf(s, format(), value().c_str());
	}
	void copy(void* ptr) { 
		strncpy((char*)ptr,val.c_str(), maxlen+1); 
		((char*)ptr)[maxlen] = '\0';
	}

	void set(const string& val) override
	{
		set_value(val);
	}
};



//
//
//  Some other types of basic_column, used mostly for time-series
// 
//

/**
	A column whose value is computed by a function
  */
template <typename T>
struct computed : basic_column
{
	static_assert(std::is_arithmetic<T>::value, 
		"Computed column on non-arithmentic type");
protected:
	std::function<T()> func;

public:
	computed(const string& _n, const string& fmt, 
		const std::function<T()>& _f) 
	: basic_column(nullptr, _n, fmt, typeid(T), sizeof(T), alignof(T)), 
		func(_f) 
	{ }

	inline T value() const { return func(); }

	void emit(FILE* s) override {
		fprintf(s, format(), value());
	}
	void copy(void* ptr) {
		T val = func();
		memcpy(ptr, &val, _size); 
	}

	bool is_arithmetic() const override { 
		return std::is_arithmetic<T>::value; 
	}
};


/**
  A column which refers to an external variable.
  */
template <typename T>
struct column_ref : basic_column
{
	static_assert(std::is_arithmetic<T>::value, 
		"column_ref on non-arithmentic type");
protected:
	T& ref;

public:
	column_ref(const string& _n, const string& fmt, T& _r) 
	: basic_column(nullptr, _n, fmt, typeid(T), sizeof(T), alignof(T)), 
		ref(_r) 
	{ }

	inline T value() const { return ref; }

	void emit(FILE* s) override {
		fprintf(s, format(), value());
	}
	void copy(void* ptr) {
		memcpy(ptr, &ref, _size); 
	}

	bool is_arithmetic() const override { 
		return std::is_arithmetic<T>::value; 
	}
};





class output_file;
class output_table;
struct output_binding;

/**
	An object that binds an output table to an output file.

	These objects are created by the `bind()` methods in `output_file`
	an `output_table`.
  */
struct output_binding 
{
	typedef std::list<output_binding*> list;
	typedef typename list::iterator iter;

	output_file* file;
	output_table* table;
	iter in_file_list, in_table_list;

	bool enabled;
	output_binding(output_file* f, output_table* t);
	~output_binding();
	static void unbind_all(list&);
	static output_binding* find(list&, output_file*);
	static output_binding* find(list&, output_table*);
};


/**
	Indicates the use for an output table
  */
enum class table_flavor {
	RESULTS,
	TIMESERIES
};


/**
	An output table.

	An output table contains a collection of columns.
	Also, an output table can be bound to one or more output files.
	During normal operations, when the `emit_row()` method is called,
	each bound output file in turn processes the current values of
	the columns to generate a row of output.

	Columns can be added to a table at initialization time.
	Once the columns are all added, and all output files are bound,
	the `prolog()` method should be called.
	After this point, it is legal to call `emit_row()`, therefore
	columns should not be changed, nor should output files be 
	bound or unbound.
	However, once the `epilog()` method is called, 
	columns can be added/removed again, and files can be bound or 
	unbound. This design allows columns to be stored separately (in other
	objects) and be added to a table in a preparation phase. 

	There are two distinct ways to orchestrate the output process.

	- A unique process that calls `emit_row()` when it decides, e.g.
	periodically during a simulation. In this model, each column is 
	kept updated independently, and is ready to be output at any time.
	Since no 'central' location needs to know the state of all columns,
	columns can be added by modules as they see fit.
	This model is particularly suited to time-series output.

	- Many processes can call `emit_output()` when they have output
	for this table. However, they should be careful to initialize
	every column of the table. 

	Output tables must have a unique, non-empty name. This name should
	probably conform to the 'identifier' convention, as it will be used
	in output formats. This is enforced by the library at creation time.

  */
class output_table : public named
{
protected:
	bool en;					// enabled flag
	std::vector<basic_column *> columns;		// the columns
	std::unordered_map<string, basic_column*> 
									colnames;	// the column names
	output_binding::list files;	// the bindings
	bool _locked;				// signal that the table allows updates
	bool _dirty;				// signal that columns have been deleted
	table_flavor _flavor;		// advice to files/formats

	inline void _check_unlocked() {
		if(_locked) throw std::logic_error("cannot modify locked output_table");
	}

	void _cleanup();
	friend struct output_binding;
	/**
	   \brief Construct an output table with given name and flavor.
	   
	   This method should probably not be used, instead construct
	   \c result_table or \c time_series objects directly.
	*/
	output_table(const string& _name, table_flavor _f);
	virtual ~output_table();

public:

	/**
	   \brief Add a column to this table.
	 */
	void add(basic_column& col);

	/**
	   \brief Remove a column to this table.
	 */
	void remove(basic_column& col);

	output_binding* bind(output_file* f) { 
		_check_unlocked();
		auto b = output_binding::find(files, f);
		if(b==nullptr)
			b = new output_binding(f, this); 
		return b;
	}

	bool unbind(output_file* f) {
		_check_unlocked();
		auto b = output_binding::find(files, f);
		if(b) delete b;
		return b;
	}

	inline auto bindings() const { return files; }

	void unbind_all() {
		_check_unlocked();
		output_binding::unbind_all(files);
	}

	inline table_flavor flavor() const { return _flavor; }
	
	inline size_t size() { 
		_cleanup();
		return columns.size(); 
	}

	inline basic_column* operator[](size_t i) { 
		_cleanup();
		return columns.at(i); 
	}

	inline basic_column* operator[](const string& n) { 
		_cleanup();
		return colnames.at(n);
	}

	inline void set_enabled(bool _en) { en=_en; }
	inline bool enabled() const { return en; }

	// data api
	void prolog();    // calls files to e.g. print a header
	void emit_row();  // a new table row is ready
	void epilog();    // calls files to e.g. print a footer

	// static members

	typedef std::unordered_set<output_table*> registry;
	
	static output_table* get(const string&);
	static const registry& all();
};



class result_table : public output_table
{
public:
	result_table(const string& _name);
	typedef std::initializer_list<basic_column *> column_list;

	result_table(const string& _name,
		column_list col);

	void add(column_list col);

	virtual ~result_table();
};


class time_series : public output_table
{
public:
	time_series(const string& _name);

	computed<dds::timestamp> now;
};


enum class open_mode { truncate, append };
const open_mode default_open_mode = open_mode::truncate;

class output_file 
{
protected:
	output_binding::list tables;
	friend struct output_binding;
public:
	output_file();
	output_file(const output_file&)=delete;
	output_file& operator=(const output_file&)=delete;

	output_file(output_file&&) = default;
	output_file& operator=(output_file&&) = default;

	virtual ~output_file();

	output_binding* bind(output_table& t) { 
		auto b = output_binding::find(tables, &t);
		if(b==nullptr)
			b = new output_binding(this, &t); 
		return b;
	}
	bool unbind(result_table& t) {
		auto b = output_binding::find(tables, &t);
		if(b) delete b;
		return b;
	}
	inline auto bindings() const { return tables; }
	void unbind_all() {
		output_binding::unbind_all(tables);
	}

	virtual void flush() { }
	virtual void close() { }
	virtual void output_prolog(output_table&)=0;
	virtual void output_row(output_table&)=0;
	virtual void output_epilog(output_table&)=0;
};



class output_c_file : public output_file
{
protected:
	FILE* stream;
	string filepath;
	bool owner;
public:

	output_c_file() : stream(0), filepath(), owner(false) {}
	output_c_file(FILE* _stream, bool _owner=false);
	output_c_file(const string& _fpath, 
		open_mode mode = default_open_mode);

	inline output_c_file(output_c_file&& other) 
	: stream(other.stream), filepath(other.filepath), owner(other.owner)
	{ other.stream = nullptr; }

	inline output_c_file& operator=(output_c_file&& other)
	{
		stream = other.stream;
		filepath = other.filepath;
		owner = other.owner;
		other.stream = nullptr;
		return *this;
	}

	virtual ~output_c_file();

	virtual void open(const string& _fpath, 
			open_mode mode = default_open_mode);
	virtual void open(FILE* _stream, bool _owner);
	virtual void close();
	virtual void flush();

	inline FILE* file() const { return stream; }
	void set_owner(bool b) { owner = b; }
	bool is_owner() const { return owner; }

	const string& path() const { return filepath; }

	virtual void output_prolog(output_table&);
	virtual void output_row(output_table&);
	virtual void output_epilog(output_table&);
private:
	void emit(basic_column*);
};

extern output_c_file output_stdout;
extern output_c_file output_stderr;

/**
	\brief A text output stream writing to a memory region.

	This is mostly useful for internal purposes, such as
	debugging.
 */
class output_mem_file : public output_c_file
{
	struct memstate {
		char* buffer;
		size_t len;		
	};
	memstate* state;
public:
	output_mem_file();
	~output_mem_file();

	inline output_mem_file& operator=(output_mem_file&& other)
	{
		output_c_file::operator=((output_c_file&&)other);
		state = other.state;
		other.state = nullptr;
		return *this;
	}

	/**
		\brief Get a pointer to the current contents of the
		stream.

		Note that this pointer should not be used after more data
		is added to the stream.
	*/
	const char* contents();

	/**
		Get a copy of the data in the stream.
	  */
	string str();
};


/**
 * \brief Progress bar.
 *
 * This is a utility class printing a progress bar on the terminal.
 * At construction, a total number of ticks, N, is specified.
 * Then, as method tick() is called the progress bar is displayed.
 *
 */
class progress_bar
{
	typedef unsigned long long llint;
	FILE* stream;
	string message;
	size_t N,i,ni;
	llint B,l;
	bool finished;


	inline llint nexti() const { return (N*(l+1)+B-1)/B; }

	void adjustBar();

public:
	/**
	 * Construct the object.
	 * @param s logger to use for output.
	 * @param b an optional length for the progress bar, in characters.
	 * @param msg an optional message to print before the bar.
	 */
	progress_bar(FILE* , size_t b=40, const string& msg="");

	/**
	 * Start printing the bar and expect the given number of ticks.
	 * @param n total number of ticks expected
	 */
	void start(unsigned long long n);

	/**
	 * This method signals ticks to the progress bar.
	 */
	inline void tick(size_t ticks = 1) {
		if(finished) return;
		if( (i += ticks)>=ni )
        	adjustBar();
	}

    /**
     * If the parameter is greater than the current number of ticks,
     * set the ticks to the given value.
     */
    inline void complete(size_t ticks) {
            if(finished) return;
            if((ticks > ni) && (ticks>i)) tick(ticks-i);
    }

    /**
     * Finish the bar now, possibly early.
     */
    void finish();
};



/**
	Output to an HDF5 file.

	All tables bound to this, will be created as HDF5 datasets into
	one HDF5 group.
  */
class output_hdf5 : public output_file
{
public:
	int locid;
	open_mode mode;

	struct table_handler;
	std::map<output_table*, table_handler*> _handler;
	table_handler* handler(output_table&);

	/**
		Use the location specified by the HDF5 id for the output
	  */
	output_hdf5(int _locid, open_mode mode=default_open_mode);

	/**
		Use the file root for the output.
	  */
	output_hdf5(const H5::H5File& _fg, open_mode mode=default_open_mode);

	/**
		Use the group for the output.
	  */
	output_hdf5(const H5::Group& _fg, open_mode mode=default_open_mode);

	/**
		Create a new HDF5 file (truncating it if needed) 
		and place the output in the root group.
	  */
	output_hdf5(const string& h5file, open_mode mode=default_open_mode);

	virtual void output_prolog(output_table&);
	virtual void output_row(output_table&);
	virtual void output_epilog(output_table&);

	~output_hdf5();
};




} // end namespace dds


#endif
