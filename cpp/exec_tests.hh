#ifndef __EXEC_TESTS_HH__
#define __EXEC_TESTS_HH__

#include "method.hh"

#include <cxxtest/TestSuite.h>

using namespace dds;

class ExecTestSuite : public CxxTest::TestSuite
{
public:
 
	void test_context()
	{
		int x=0;

		auto r = ON(INIT, [&x](){ x=1; });
		CTX.run();
		TS_ASSERT_EQUALS(x,1);

		CTX.cancel_rule(r);
		x=0;
		CTX.run();

		TS_ASSERT_EQUALS(x,0);		
	}

};

#endif