#ifndef __METHOD_HH___
#define __METHOD_HH___

#include <vector> 

#include "dds.hh"
#include "data_source.hh"

namespace dds {

/**
	Base class for executable methods.
  */
class exec_method
{
public:
	virtual void start()  {}
	virtual void process(const dds_record&) {}
	virtual void finish() {}
	virtual ~exec_method() {}
};


/**
	Method that estimates a query.
  */
class estimating_method
{
	
};



/**
	An executor is essentially a loop, feeding a
	distributed stream to all the methods.
  */
class executor
{
protected:
	data_source* src;
	std::vector<exec_method*> methods;
public:
	executor(data_source* ds);
	virtual ~executor();

	inline void add(exec_method* method) {
		methods.push_back(method);
	}

	void run();
};


} //end namespace dds

#endif
