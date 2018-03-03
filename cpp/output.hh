#ifndef __OUTPUT_HH__
#define __OUTPUT_HH__

#include <string>
#include <vector>
#include <cstdio>
#include <cstring>
#include <list>
#include <map>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <algorithm>
#include <typeinfo>
#include <typeindex>

#include "binc.hh"


namespace H5
{
	struct Group;
	struct H5File;
}

namespace tables
{

using std::string;
using std::type_index;
using std::type_info;

using binc::named;
using binc::enum_repr;

/*-----------------------------
	Generic classes
  -----------------------------*/

class output_table;

/**
	\brief The base class for table columns.

	Column objects encapsulate a name, a data type and some presentation
	metadata, corresponding to a table column. Also, at each time, column
	objects hold a value of their data type.
	When a table row is emitted, all columns are accessed for their current
	value.

	A column may be added to a table at a later stage. This is done from
	the table side.	Because clumns can be bound to a table, they are non-copyable, 
	non-movable objects.
  */
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

	/**
		\brief Construct a column
		@param _tab the table to put the column in
		@param _name name of the column
		@param f the \c printf format for this column 
		@param _t a \c type_info for this column
		@param _s the binary for the column type
		@param _a the binary alignment for the column type
	  */
	basic_column(
		output_table* _tab, const string& _name, const string& f, 
		const type_info& _t, size_t _s, size_t _a);

	basic_column(const basic_column&)=delete;
	basic_column(const basic_column&&)=delete;

	/**
		\brief Destroy the column.

		This will remove it from the table it belong to
	  */
	virtual ~basic_column();

	/**
		\brief The table of this column
	  */
	inline output_table* table() const { return _table; }

	/**
		\brief Index of column in the table
	  */
	inline size_t index() const { return _index; }

	/**
		\brief The column's preferred text format
	  */
	inline const char* format() const { return _format.c_str(); }

	/**
		\brief Output this column to a text file

		This must be overridden by subclasses
	  */
	virtual void emit(FILE*) = 0;

	/**
		\brief Copy the binary representation to a location
	  */
	virtual void copy(void*) = 0;

	/**
		\brief Return true if the column type is arithmetic
	  */
	virtual bool is_arithmetic() const { return false; }

	/**
		\brief Return the type index of the column type
	  */
	inline type_index type() const { return _type; }

	/**
		\brief
	  */
	inline size_t size() const { return _size; }

	/**
		\brief
	  */
	inline size_t align() const { return _align; }

	// Using these, one can set a basic_column if only it is known
	// whether it is arithmetic or not.
	// The default implementation throws invalid_argument

	/**
		\brief
	  */
	virtual void set(double val);

	/**
		\brief
	  */
	virtual void set(const string&);
};



/**
	\brief  A typed table column

	A typed column is used to make column use syntactically convenient.
	This is a concrete class.

	@tparam the type of the column 
  */
template <typename T>
class column : public basic_column
{
	static_assert(std::is_arithmetic<T>::value, 
		"Column on non-arithmentic type");
protected:
	T val;
public:

	/**
		\brief Construct a column bound to a table
		@param _tab the table to hold the column
		@param _n   the column name
		@param fmt  the column's text format 
	  */
	column(output_table* _tab, const string& _n, const string& fmt) 
	: basic_column(_tab, _n, fmt, typeid(T), sizeof(T), alignof(T)) { }

	/**
		\brief Construct an unbound column
		@param _n   the column name
		@param fmt  the column's text format 
	  */
	column(const string& _n, const string& fmt) 
	: basic_column(nullptr, _n, fmt, typeid(T), sizeof(T), alignof(T)) { }

	/**
		\brief Construct a column bound to a table and initialize its value
		@param _tab the table to hold the column
		@param _n   the column name
		@param fmt  the column's text format 
		@param _v   the initial value
	  */
	column(output_table* _tab, const string& _n, const string& fmt, const T& _v) 
	: basic_column(_tab, _n, fmt, typeid(T), sizeof(T), alignof(T)), val(_v) { }

	/**
		\brief Construct an unbound column and initialize its value
		@param _n   the column name
		@param fmt  the column's text format 
		@param _v   the initial value
	  */
	column(const string& _n, const string& fmt, const T& _v) 
	: basic_column(nullptr, _n, fmt, typeid(T), sizeof(T), alignof(T)), val(_v) { }

	/**
		\brief The current column value
	  */
	inline T value() const { return val; }

	/**
		\brief A reference to the current value.
		This reference can be used to change the value
	  */
	inline T& value() { return val; }

	/**
		\brief Set the column value via assignment.

		This is eqivalent to \c col.value()=v

		@param v the new column value
	  */
	inline void set_value(const T& v) { val=v; }

	/**
		\brief Set the column value via assignment.

		This is eqivalent to \c col.value()=v

		@param v the new column value
		@return a reference to the column value
	  */
	inline T& operator=(T v) { val=v; return val; }

	/**
		\brief Output the column value to a text file
		@param s the text file to write to
	  */
	void emit(FILE* s) override {
		fprintf(s, format(), value());
	}

	/**
		\brief Copy the binary value of the column to a location
		@param ptr location to copy the column value to
	  */
	void copy(void* ptr) { memcpy(ptr, &val, _size); }

	/**
		\brief Set the column value to an arithmetic value.

		If the column type is not arithmetic, a compile-time error
		will occur if this method is called
	  */
	void set(double val) override
	{
		set_value(val);
	}

	/**
		\brief Check if the type of this column is arithmetic
	  */
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

	/**
		\brief Construct an unbound column.

		@param _n the column name
		@param fmt the column \c printf format
		@param _f a function object to provide the value
	  */
	computed(const string& _n, const string& fmt, 
		const std::function<T()>& _f) 
	: basic_column(nullptr, _n, fmt, typeid(T), sizeof(T), alignof(T)), 
		func(_f) 
	{ }

	/**
		\brief Return the column value 
		@return the current value of the column
	  */
	inline T value() const { return func(); }

	/**
		\brief Emit the column value to a text file
	  */
	void emit(FILE* s) override {
		fprintf(s, format(), value());
	}

	/**
		\brief Copy the column value to a location
	  */
	void copy(void* ptr) {
		T val = func();
		memcpy(ptr, &val, _size); 
	}

	/**
		\brief Return true if the column type is arithmetic
	  */
	bool is_arithmetic() const override { 
		return std::is_arithmetic<T>::value; 
	}
};


/**
  	A column which refers to an external variable.

  	Such a column can be bound to program variables
  	so that their value may be reported via a table
  	(most likely, a time-series).

  	Thus, this type of column may be thought of as a
  	_trace_ on a program variable.
  */
template <typename T>
struct column_ref : basic_column
{
	static_assert(std::is_arithmetic<T>::value, 
		"column_ref on non-arithmentic type");
protected:
	T& ref;

public:

	/**
		\brief Construct an unbound column

		@param _n the column name
		@param fmt the column format
		@param _r reference to the variable holding the column value
	  */
	column_ref(const string& _n, const string& fmt, T& _r) 
	: basic_column(nullptr, _n, fmt, typeid(T), sizeof(T), alignof(T)), 
		ref(_r) 
	{ }

	/**
		\brief Return the current value of the 
	  */
	inline T value() const { return ref; }

	/**
		\brief Print the current value to a text file
	  */
	void emit(FILE* s) override {
		fprintf(s, format(), value());
	}

	/**
		\brief Copy the current value to a location
	  */
	void copy(void* ptr) {
		memcpy(ptr, &ref, _size); 
	}

	/**
		\brief Check if the column type is arithmetic
	  */
	bool is_arithmetic() const override { 
		return std::is_arithmetic<T>::value; 
	}
};



/**
  	A column which refers to an external variable.

  	Such a column can be bound to program variables
  	so that their value may be reported via a table
  	(most likely, a time-series).

  	Thus, this type of column may be thought of as a
  	_trace_ on a program variable.
  */
template <>
struct column_ref<string> : basic_column
{
protected:
	const size_t maxlen;
	string& ref;
public:

	/**
		\brief Construct a string-reference column

		@param _n the column name
		@param _maxlen the maximum length for this column
		@param fmt the column format
		@param _r reference to the variable holding the column value
	  */
	column_ref(const string& _n, size_t _maxlen, const string& fmt, string& _r) 
	: basic_column(nullptr, _n, fmt, 
		typeid(string), sizeof(char[_maxlen+1]), alignof(char[_maxlen+1])),
		maxlen(_maxlen), ref(_r) 
	{ }


	/**
		\brief Construct a string-reference column

		@param _n the column name
		@param _maxlen the maximum length for this column
		@param fmt the column format
		@param _r reference to the variable holding the column value
	  */
	column_ref(output_table* _tab, const string& _n, size_t _maxlen, const string& fmt, string& _r) 
	: basic_column(_tab, _n, fmt, 
		typeid(string), sizeof(char[_maxlen+1]), alignof(char[_maxlen+1])),
		maxlen(_maxlen), ref(_r) 
	{ }


	/**
		\brief Return the current value of the 
	  */
	inline const string& value() const { return ref; }

	/**
		\brief Print the current value to a text file
	  */
	void emit(FILE* s) override {
		fprintf(s, format(), value().c_str());
	}

	/**
		\brief Copy the current value to a location
	  */
	void copy(void* ptr) {
		strncpy((char*)ptr, ref.c_str(), maxlen+1);
        ((char*)ptr)[maxlen] = '\0';
	}

	/**
		\brief Check if the column type is arithmetic
	  */
	bool is_arithmetic() const override { 
		return std::is_arithmetic<string>::value; 
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
	RESULTS,    //< Indicate a normal table of results after a run
	TIMESERIES  //< Indicate a time-series table, of data collected during a run
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

	/**
		\brief Bind this table to an output_file
	  */
	output_binding* bind(output_file* f) { 
		_check_unlocked();
		auto b = output_binding::find(files, f);
		if(b==nullptr)
			b = new output_binding(f, this); 
		return b;
	}

	/**
		\brief Unind this table from an output_file.

		If the table is not bound to the file, there is no
		effect
	  */
	bool unbind(output_file* f) {
		_check_unlocked();
		auto b = output_binding::find(files, f);
		if(b) delete b;
		return b;
	}

	/**
		\brief Return all the bindings for this table.
	  */
	inline auto bindings() const { return files; }

	/**
		\brief Unbind from all files
	  */
	void unbind_all() {
		_check_unlocked();
		output_binding::unbind_all(files);
	}

	/**
		\brief Return the table flavor (results or time_series)
	  */
	inline table_flavor flavor() const { return _flavor; }
	
	/**
		\brief Return the number of columns of this table
	  */
	inline size_t size() { 
		_cleanup();
		return columns.size(); 
	}

	/**
		\brief Return a column by index
	  */
	inline basic_column* operator[](size_t i) { 
		_cleanup();
		return columns.at(i); 
	}

	/**
		\brief Return a column by name
	  */
	inline basic_column* operator[](const string& n) { 
		_cleanup();
		return colnames.at(n);
	}

	/**
		\brief Set the enabled flag of the table

		A disabled table does not emit any data, even if
		\c emit_row() is called.

		@param _en the value of the enabled flag
	  */
	inline void set_enabled(bool _en) { en=_en; }

	/**
		\brief Return the enabled flag of the table
	  */
	inline bool enabled() const { return en; }

	// data api

	/**
		\brief Put a table to output mode.

		A call to this method must be made before any data is output,
		but only after all columns have been added. 
	  */
	void prolog();    // calls files to e.g. print a header

	/**
		\brief Emit a table row

		Data for the table row is collected from the column objects
	  */
	void emit_row();  // a new table row is ready

	/**
		\brief Terminate output mode, make table editable again

		A call to this method must follow all output operations
	  */
	void epilog();    // calls files to e.g. print a footer

	// static members

	typedef std::unordered_set<output_table*> registry;
	
	/**
		\brief Get a table object by name
	  */
	static output_table* get(const string&);

	/**
		\brief A container for all table objects
	  */
	static const registry& all();
};


/**
	\brief Table for data reported after the end of a run

	A result table is a flavor of \c output_table which specializes
	in reporting data after the end of a run.
  */
class result_table : public output_table
{
public:
	/**
		\brief Construct a table
		@param _name the table name
	  */
	result_table(const string& _name);

	/**
		Initializer-list of pointers to columns
	  */
	typedef std::initializer_list<basic_column *> column_list;

	/**
		\brief Construct a table and add columns
		@param _name the table name
		@param col a list of column objects to add
	  */
	result_table(const string& _name,
		column_list col);

	/**
		\brief Add a set of column ojects to the table
	  */
	void add(column_list col);

	/**
		\brief Destroy the result table
	  */
	virtual ~result_table();
};


/**
	\brief Table for data collected during a run

	A time series table collects a number of column values during a run
  */
template <typename TimeType>
class time_series : public output_table
{
public:
	/**
		\brief Construct a time series table
		@param _name the table name
	  */
	time_series(const string& _name, const string& _nowfmt, const std::function<TimeType()>& _nowfunc)
	: output_table(_name, table_flavor::TIMESERIES), 
		now("time", _nowfmt, _nowfunc) 
	{ add(now); }

	/**
		\brief The type of the time column. 
		*/
	typedef TimeType time_type;

	/**
		\brief A column for this table containing the current stream time

		This column is the first column of the time series table
	  */
	computed<TimeType> now;
};


/**
	\brief Open mode fore new output files

	When creating a new file object, the values of this \c enum are
	used to indicate whether to open the file in _truncate_ or _append_ mode.
  */
enum class open_mode { truncate, append };

/**
	\brief Default open mode
  */
const open_mode default_open_mode = open_mode::truncate;

/**
	\brief  Abstract base class for an output file

	Output files are used to write \c output_table data to the
	filesystem
  */
class output_file 
{
protected:
	output_binding::list tables;
	friend struct output_binding;
public:

	/**
		\brief Constructor
	  */
	output_file();

	output_file(const output_file&)=delete;
	output_file& operator=(const output_file&)=delete;
	output_file(output_file&&) = default;
	output_file& operator=(output_file&&) = default;

	/**
		\brief Destructor
	  */
	virtual ~output_file();


	/**
		\brief Bind this file to a table
		@param t the table to bind with
		@return the binding object
	  */
	output_binding* bind(output_table& t) { 
		auto b = output_binding::find(tables, &t);
		if(b==nullptr)
			b = new output_binding(this, &t); 
		return b;
	}

	/**
		\brief Unbind this file from a table
	  */
	bool unbind(result_table& t) {
		auto b = output_binding::find(tables, &t);
		if(b) delete b;
		return b;
	}

	/**
		\brief All bindings
	  */
	inline auto bindings() const { return tables; }

	/**
		\brief Unbind this file from all tables
	  */
	void unbind_all() {
		output_binding::unbind_all(tables);
	}

	/**
		\brief Flush this file
	  */
	virtual void flush() { }

	/**
		\brief Close this file
	  */
	virtual void close() { }

	/**
		\brief Called by a table to prepare for output mode

	  */
	virtual void output_prolog(output_table&)=0;

	/**
		\brief Output a table row
	  */
	virtual void output_row(output_table&)=0;

	/**
		\brief Conclude the output session
	  */
	virtual void output_epilog(output_table&)=0;
};


/**
	\brief Specify the format of text files

	\c csvtab Formats rows without a header, which is 
	a problem for multi-table output. Columns are separated
	by comma. There is a header at the top row

	\c csvrel Each row is prefixed by the table name. All
	row values are separated by ,
  */
enum class text_format {
	csvtab,    
	csvrel
};
const text_format default_text_format = text_format::csvrel;

// forward
struct output_c_file;

/**
	\brief Base class for text file formatters
  */
struct formatter {
protected:
	output_c_file* ofile;
	output_table& table;

public:
	formatter(output_c_file* of, output_table& tab);
	virtual ~formatter();
	virtual void prolog()=0;
	virtual void row()=0;
	virtual void epilog()=0;

	// static factory
	static formatter* create(output_c_file*,output_table& t, text_format fmt);
	static void destroy(formatter*);
};


/**
	\brief An output file for text files via the
	C standard library.
  */
class output_c_file : public output_file
{
protected:

	FILE* stream;
	string filepath;
	bool owner;
	text_format fmt;
	std::unordered_map<output_table*,formatter*> fmtr;

public:

	/**
		\brief Construct without a stream
		@param _fmt the format
	  */
	output_c_file(text_format _fmt=default_text_format) 
		: stream(0), filepath(), owner(false), fmt(_fmt) {}

	/**
		\brief Create an \c output_c_file on an existing stream
		@param _stream the stream to use
		@param _owner whether to call \c fclose at the end or not
		@param _fmt the format
	  */
	output_c_file(FILE* _stream, bool _owner, text_format _fmt=default_text_format);

	/**
		\brief Create an \c output_c_file for a given filename
		@param _fpath the file name to open
		@param mode the open mode
		@param _fmt the format
	  */
	output_c_file(const string& _fpath, 
		open_mode mode = default_open_mode, text_format _fmt=default_text_format);

	/**
		\brief Move constructor
	  */
	inline output_c_file(output_c_file&& other) 
	: stream(other.stream), filepath(other.filepath), owner(other.owner), fmt(other.fmt)
	{ other.stream = nullptr; }

	/**
		\brief Move assignment
	  */
	inline output_c_file& operator=(output_c_file&& other)
	{
		stream = other.stream;
		filepath = other.filepath;
		owner = other.owner;
		other.stream = nullptr;
		return *this;
	}

	/**
		\brief Destructor
		This will call \c close() before destroying the object
	  */
	virtual ~output_c_file();

	/**
		\brief Open a new C stream for this object
		@param _fpath the file path to use
		@param mode the open mode
	  */
	virtual void open(const string& _fpath, 
			open_mode mode = default_open_mode);

	/**
		\brief Use an existing stream for this object
		@param _stream the stream to use
		@param _owner whether the stream is owned or not
	  */
	virtual void open(FILE* _stream, bool _owner);

	/**
		\brief Close a steam and reset the \c output_c_file object

		This method behaves differently based on whether the stream is
		owned or not. On an owned stream, \c fclose is called. On a 
		non-owned stream, \c fflush is called.
	  */
	virtual void close();

	/**
		\brief Flush the underlying stream
	  */
	virtual void flush();

	/**
		\brief The current stream object (which may be null)
	  */
	inline FILE* file() const { return stream; }

	/**
		\brief Set the owner flag for the current stream
	  */
	void set_owner(bool b) { owner = b; }

	/**
		\brief Get the owner flag for the current stream
	  */
	bool is_owner() const { return owner; }

	/**
		\brief Return the path to the current stream
	  */
	const string& path() const { return filepath; }

	/**
		\brief Prepare for output.
		@param t the \c output_table to prepare for
	  */
	virtual void output_prolog(output_table& t);

	/**
		\brief Output a row
		@param t the \c output_table to output from
	  */
	virtual void output_row(output_table&);

	/**
		\brief Finish the output session for this table
		@param t the \c output_table to finish for.
	  */
	virtual void output_epilog(output_table&);
};

/**
	\brief An \c output_c_file for the standard output.
  */
extern output_c_file output_stdout;

/**
	\brief An \c output_c_file for the standard error.
  */
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

	/**
		\brief Constructor
	  */
	output_mem_file(text_format fmt = text_format::csvtab);

	/**
		\brief Destructor
	  */
	~output_mem_file();

	/**
		\brief Move assignment
	  */
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
		@return a string with the current contents
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
	one HDF5 group. The dataset name will be the table name.
	HDF5 datasets will be created as arrays of structs ('compound types'
	in HDF5 parlance).
  */
class output_hdf5 : public output_file
{
	long int locid;
	open_mode mode;

	struct table_handler;
	std::map<output_table*, table_handler*> _handler;
	table_handler* handler(output_table&);
	friend class OutputTestSuite;
public:

	/**
		Use the location specified by the HDF5 id for the output
	  */
	output_hdf5(long int _locid, open_mode mode=default_open_mode);

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

	/**
		\brief Prepare for output from this table
	  */
	virtual void output_prolog(output_table&);

	/**
		\brief Output a row from this table
	  */
	virtual void output_row(output_table&);

	/**
		\brief Conclude the output session
	  */
	virtual void output_epilog(output_table&);

	/**
		\brief Destructor
	  */
	~output_hdf5();
};


/*-----------------------------
	I/O assistants
  -----------------------------*/


extern enum_repr<text_format> text_format_repr;
extern enum_repr<open_mode> open_mode_repr;


} // end namespace dds


#endif
