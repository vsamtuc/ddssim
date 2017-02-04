#ifndef __OUTPUT_TESTS_HH__
#define __OUTPUT_TESTS_HH__


#include "output.hh"

#include <cxxtest/TestSuite.h>

using namespace dds;

class OutputTestSuite : public CxxTest::TestSuite
{
public:

	struct _silly {
		column<int> count {"count", "%d"};
		column<double> mean_x {"mean_x", "%f" };
		column<std::string> label {"label", "%s" };
	};

	struct silly_table : _silly, result_table
	{
		silly_table(const char* n) 
		: result_table(n, {&count, &mean_x, &label}) {}
	};

	void test_output()
	{
		silly_table tab("SILLY");

		tab.count = 1;
		tab.mean_x = 3.14;
		tab.label = "Hello";

		char* buf = NULL;
		size_t len = 0;
		FILE* mf = open_memstream(&buf, &len);
		output_file* f = new output_c_file(mf);
		f->bind(tab);

		tab.prolog();
		tab.emit_row();
		tab.emit_row();
		tab.emit_row();
		tab.epilog();

		delete f;
		fclose(mf);

		const char* expected =
		"# INDEX,count,mean_x,label\n"
		"SILLY,1,3.140000,Hello\n"
		"SILLY,1,3.140000,Hello\n"
		"SILLY,1,3.140000,Hello\n";

		TS_ASSERT_EQUALS( buf, expected );
		free(buf);
	}

	void test_time_series()
	{
		time_series T("series");

		column<double> t1 { "t1", "%f" };
		column<double> t2 { "t2", "%f" };

		T.add(t1);
		T.add(t2);

		//T.emit_header(stdout);

		t1 = 13.2;
		t2 = 11.4;
		for(dds::timestamp t=10; t < 12; t++) {
			T.now = t;
			t1 = t1.value() + t;
			t2 = t2.value() - t;
			//T.emit(stdout);
		}
	}


	void test_output_file() 
	{
		output_c_file* fset[2];

		fset[0] = new output_c_file(fmemopen(NULL, 8192, "w+"),true);
		fset[1] = new output_c_file(fmemopen(NULL, 8192, "w+"),true);

		silly_table tab("SILLY");

		tab.count = 1;
		tab.mean_x = 3.14;
		tab.label = "Hello";

		tab.bind_all(fset);
		tab.prolog();
		tab.emit_row();

		char buf[2][8192];
		fseek(fset[0]->file(), 0, SEEK_SET);
		fseek(fset[1]->file(), 0, SEEK_SET);
		TS_ASSERT_EQUALS(fread(buf[0], 1, 8191, fset[0]->file()), 50);
		TS_ASSERT_EQUALS(fread(buf[1], 1, 8191, fset[1]->file()), 50);
		buf[0][50] = buf[1][50] = 0;
		TS_ASSERT_EQUALS( buf[0], buf[1] );
		auto expected = "# INDEX,count,mean_x,label\n"
						"SILLY,1,3.140000,Hello\n";
		TS_ASSERT_EQUALS( buf[0], expected );

		delete fset[0];
		delete fset[1];
	}

	void test_bind() 
	{
		silly_table T1("table1"), T2("table2");

		output_file* f1 = new output_c_file("/dev/null");
		output_file* f2 = new output_c_file("/dev/null");
		output_file* f3 = new output_c_file("/dev/null");

		T1.bind(f1);
		TS_ASSERT_EQUALS(T1.bindings().size(), 1);
		TS_ASSERT_EQUALS(T2.bindings().size(), 0);
		TS_ASSERT_EQUALS(f1->bindings().size(), 1);

		T1.bind(f1);
		TS_ASSERT_EQUALS(T1.bindings().size(), 1);
		TS_ASSERT_EQUALS(f1->bindings().size(), 1);

		f1->bind(T1);
		TS_ASSERT_EQUALS(T1.bindings().size(), 1);
		TS_ASSERT_EQUALS(f1->bindings().size(), 1);

		f2->bind(T1);
		T1.bind(f3);
		f1->bind(T2);
		TS_ASSERT_EQUALS(T1.bindings().size(), 3);
		TS_ASSERT_EQUALS(T2.bindings().size(), 1);
		TS_ASSERT_EQUALS(f1->bindings().size(), 2);
		TS_ASSERT_EQUALS(f2->bindings().size(), 1);
		TS_ASSERT_EQUALS(f3->bindings().size(), 1);

		f2->unbind(T2);
		TS_ASSERT_EQUALS(T1.bindings().size(), 3);
		TS_ASSERT_EQUALS(T2.bindings().size(), 1);
		TS_ASSERT_EQUALS(f1->bindings().size(), 2);
		TS_ASSERT_EQUALS(f2->bindings().size(), 1);
		TS_ASSERT_EQUALS(f3->bindings().size(), 1);

		delete f3;
		TS_ASSERT_EQUALS(T1.bindings().size(), 2);
		TS_ASSERT_EQUALS(T2.bindings().size(), 1);
		TS_ASSERT_EQUALS(f1->bindings().size(), 2);
		TS_ASSERT_EQUALS(f2->bindings().size(), 1);

		delete f1;
		delete f2;
		TS_ASSERT_EQUALS(T1.bindings().size(), 0);
		TS_ASSERT_EQUALS(T2.bindings().size(), 0);
	}


};


#endif
