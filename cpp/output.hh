#ifndef __OUTPUT_HH__
#define __OUTPUT_HH__

#include <string>
#include <vector>
#include <cstdio>
#include <list>
#include <algorithm>

#include "dds.hh"

namespace dds
{

using std::string;


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
	friend class output_table;
public:
	basic_column(const string& _name, const string& f)
	: named(_name), _table(0), _format(f) { }

	basic_column(const basic_column&)=delete;
	basic_column(const basic_column&&)=delete;
	virtual ~basic_column();

	inline const char* format() const { return _format.c_str(); }
	virtual void emit(FILE*) = 0;
};


template <typename T>
class column : public basic_column
{
protected:
	T val;
public:
	column(const string& _n, const string& fmt) 
	: basic_column(_n, fmt) { }

	column(const string& _n, const string& fmt, const T& _v) 
	: basic_column(_n, fmt), val(_v) { }

	inline T value() const { return val; }
	inline T& value() { return val; }
	inline T& operator=(T v) { val=v; return val; }
	void emit(FILE* s) override {
		fprintf(s, format(), value());
	}
};

template <>
class column<string> : public basic_column
{
protected:
	string val;
public:
	column(const string& _n, const string& fmt) 
	: basic_column(_n, fmt) { }

	column(const string& _n, const string& fmt, const string& _v) 
	: basic_column(_n, fmt), val(_v) { }

	inline const string& value() const { return this->val; }
	inline string& value() { return val; }
	inline string& operator=(const string& v) { val=v; return val; }
	void emit(FILE* s) override {
		fprintf(s, format(), value().c_str());
	}
};

class output_file;
class output_table;
struct __binding;

struct __binding 
{
	typedef std::list<__binding*> list;
	typedef typename list::iterator iter;

	output_file* file;
	output_table* table;
	iter in_file_list, in_table_list;

	bool enabled;
	__binding(output_file* f, output_table* t);
	~__binding();
	static void unbind_all(list&);
	static __binding* find(list&, output_file*);
	static __binding* find(list&, output_table*);
};

/**
	Indicates the use for an output table
  */
enum class table_flavor {
	RESULTS,
	TIMESERIES
};

class output_table : public named
{
protected:
	bool en;
	std::vector<basic_column *> columns;
	friend class basic_column;
	inline void __associate(basic_column* col, size_t _i) {
		if(col->_table)
			throw std::runtime_error("column already added to a table");
		col->_table = this;
		col->_index = _i;
	}

	__binding::list files;
	friend struct __binding;

	table_flavor _flavor;
public:
	output_table(const string& _name, table_flavor _f);
	virtual ~output_table();

	inline void add(basic_column& col) {
		__associate(&col, columns.size());
		columns.push_back(&col); 
	}

	void bind(output_file* f) { 
		if(__binding::find(files, f)==0)
			new __binding(f, this); 
	}

	template <typename T>
	void bind_all(T& ctr) {
		std::for_each(std::begin(ctr), std::end(ctr), 
			[&](output_file* f){ bind(f); } );
	}

	void unbind(output_file* f) {
		auto b = __binding::find(files, f);
		if(b) delete b;
	}

	inline auto bindings() const { return files; }

	void unbind_all() {
		__binding::unbind_all(files);
	}

	inline table_flavor flavor() const { return _flavor; }
	
	inline size_t size() const { return columns.size(); }
	inline basic_column* operator[](size_t i) const { return columns.at(i); }

	inline void set_enabled(bool _en) { en=_en; }
	inline bool enabled() const { return en; }

	// data api
	void prolog();    // calls files to e.g. print a header
	void emit_row();  // a new table row is ready
	void epilog();    // calls files to e.g. print a footer
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
	time_series();
	time_series(const string& _name);

	column<dds::timestamp> now;
};


enum open_mode { truncate, append };
const open_mode default_open_mode = open_mode::truncate;

class output_file 
{
protected:
	__binding::list tables;
	friend struct __binding;
public:
	output_file();
	output_file(const output_file&)=delete;
	output_file& operator=(const output_file&)=delete;

	output_file(output_file&&) = default;

	virtual ~output_file();

	void bind(output_table& t) { 
		if(__binding::find(tables, &t)==0)
			new __binding(this, &t); 
	}
	void unbind(result_table& t) {
		auto b = __binding::find(tables, &t);
		if(b) delete b;
	}
	inline auto bindings() const { return tables; }
	void unbind_all() {
		__binding::unbind_all(tables);
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





} // end namespace dds


#endif