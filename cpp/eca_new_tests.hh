#ifndef __ECA_TESTS_HH__
#define __ECA_TESTS_HH__

#include <functional>
#include <typeinfo>
#include <regex>
#include <cxxtest/TestSuite.h>

#include "eca_new.hh"

using namespace std;
using namespace eca;


constexpr event_type<int> hello("hello");


template <typename Arg>
void action(event<Arg> evt)
{
	cout << evt.arg << endl;
}


class EcaTestSuite : public CxxTest::TestSuite
{
public:

	void test_factory() {

		event<int> evt = hello(6);
		
		action(evt);
	}

};


#endif
