#ifndef __METHOD_HH___
#define __METHOD_HH___

#include <vector> 
#include <map>

#include "dds.hh"
#include "data_source.hh"

namespace dds {

class executor;


/**
	Base class for executable methods.
  */
class exec_method
{
protected:
	executor* _exc;
public:
	inline executor* exc() const { return _exc; }
	timestamp now() const;

	virtual void start(executor& exc)  { _exc = &exc; }
	virtual void process(const dds_record&) {}
	virtual void rolling_report() {}
	virtual void finish() {}
	virtual ~exec_method() {}
};


/**
	Method that estimates a query.
  */
class estimating_method : public exec_method
{
public:
	virtual void process(const dds_record& rec) override;

	virtual double current_estimate() const =0;
	virtual void process_record(const dds_record&) =0;

};


/**
	An exact method estimates a query exactly.

	This method should be added last in the executor.
	At each cycle, it will compute the error between
	its own (exact) estimate and the estimate of
	other methods in the executor.
  */
class exact_method : public estimating_method
{
	struct observer;

	std::vector<observer*>	observers;
	size_t window = 4;
public:
	virtual void start(executor& exc) override;
	virtual void process(const dds_record&) override;
	virtual void finish() override;
	virtual ~exact_method();
};


/**
	An executor is essentially a loop, feeding a
	distributed stream to all the methods.
  */
class executor
{
protected:
	data_source* src;
	std::vector<exec_method*> meths;
	timestamp _now;
public:
	executor(data_source* ds);
	virtual ~executor();

	inline timestamp now() const { return _now; }

	inline void add(exec_method* method) {
		meths.push_back(method);
	}

	inline size_t methods() const { return meths.size(); }
	inline exec_method* get_method(size_t i) const {
		return meths.at(i);
	}

	void run();
};


inline timestamp exec_method::now() const { return _exc->now(); }


} //end namespace dds

#endif
