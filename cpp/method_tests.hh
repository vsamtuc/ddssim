#ifndef __OUTPUT_TESTS_HH__
#define __OUTPUT_TESTS_HH__

#include <functional>
#include <typeinfo>
#include <regex>

#include "method.hh"
#include "binc.hh"
#include "cfgfile.hh"

#include <cxxtest/TestSuite.h>
#include "hdf5_util.hh"


using binc::print;
using namespace dds;


/*
  N.B. Some day, write up an injection facility...

 */

template <typename T, typename ...Args>
T* inject(Args...args)
{
	typedef factory<T, Args...> factory_type;
	
	auto factype = typeid(factory_type);
	return nullptr;
}



template <typename Comp>
class injected;

template <typename Comp>
struct injector
{
	virtual Comp* get() =0;
	virtual ~injector() { }
};

template <typename Comp>
struct named_injector : injector<Comp>
{
	string name;
	named_injector(const string& _name) : name(_name) { }
	Comp* get() override {
		return nullptr;
	}	
};

template <typename Comp, typename ...Args>
struct factory_injector : injector<Comp>
{
	typedef factory<Comp, Args...> factory_type;
	factory_type& fct;
	typename factory_type::index_type cargs;

	factory_injector(factory_type& _fct, Args...args) 
	: fct(_fct), cargs(args...) 
	{ }
	Comp* get() override {
		return fct.get(cargs);
	}	
};



template <typename Comp>
class injected
{
	component* _owner;
	mutable Comp* _ptr = nullptr;
	injector<Comp>* _injector;

	void inject() const {
		if(_ptr==nullptr)
			_ptr = _injector->get();
	}

public:
	injected(component* _own, const string& _cname)
	: _owner(_own), _injector(new named_injector<Comp>(_cname)) 
	{ }

	template <typename ...Args>
	injected(component* _own, factory<Comp,Args...>& fac, Args..._args )
	: _owner(_own), _injector(new factory_injector<Comp, Args...>(fac, _args...))
	{ }

	Comp* operator->() const { inject(); return _ptr; }
	Comp* operator*() const { inject(); return _ptr; }
};


/*
template <>
inline agms_sketch_updater* 
inject<agms_sketch_updater, stream_id, agms::projection>(stream_id sid, 
	agms::projection proj)
{
	return agms_sketch_updater_factory(sid, proj);
}
*/


struct Bar : component
{
	int x;
	Bar(int _x) : x(_x) { }
};


factory<Bar, int> barfactory;



struct Foo : component
{
	injected<dataset> dset { this, "dataset" };

	injected<Bar> bar { this, barfactory, 5 };

	Foo() : component("foo1") { }
};


enum class Evts
{  E1, E2, E3 };


template <enum Evts>
struct EVT 
{

};



class MethodTestSuite : public CxxTest::TestSuite
{
public:

	void test_factory() {
		Foo foo;

		EVT<Evts::E1> e1, e2;
		EVT<Evts::E2> e3, e4;

		Bar* bar = barfactory(3);

		TS_ASSERT_EQUALS( * foo.dset , nullptr );
		TS_ASSERT_DIFFERS( * foo.bar , nullptr );
		TS_ASSERT_EQUALS( foo.bar->x , 5 );

		TS_ASSERT_EQUALS( bar->x, 3);
	}

};


#endif
