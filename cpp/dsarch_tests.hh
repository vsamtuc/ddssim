
#include <memory>
#include <string>
#include <boost/range/adaptors.hpp>

#include <cxxtest/TestSuite.h>
class ArchTestSuite;
#define DSARCH_CXXTEST_RUNNING
#include "dsarch.hh"
#include "binc.hh"



using namespace dds;
using std::string;
using binc::print;

struct Echo : process
{
	int value;

	Echo(basic_network* nw) : process(nw), value(0) {}

	// remote methods

	string echo(const string& msg) {
		return string("Echoing ")+msg;
	}

	Acknowledge<int> send_int(int i) { 
		value=i; 
		if(value<0) return NAK;
		return i+1;
	}
	int get_int() { return value; }

	Ack init() { return NAK; }

	oneway say_bye(const string& msg) { value=-1; }
	oneway finish() {  }

	int add(int x, int y) { return x+y; }
};


struct Echo_proxy : remote_proxy<Echo>
{
	REMOTE_METHOD(Echo, echo);
	REMOTE_METHOD(Echo, send_int);
	REMOTE_METHOD(Echo, get_int);
	REMOTE_METHOD(Echo, init);
	REMOTE_METHOD(Echo, say_bye);
	REMOTE_METHOD(Echo, finish);
	REMOTE_METHOD(Echo, add);

	Echo_proxy(process* owner) 
	: remote_proxy<Echo>(owner)
	{}
};

struct Echo_cli : process
{
	Echo_proxy proxy;

	Echo_cli(basic_network* nw) 
	: process(nw), proxy(this)
	{
		//proxy = new Echo_proxy(this, srv);
	}

	string send_echo(string msg) {
		proxy.init();
		proxy.send_int(10);
		return proxy.echo(msg);		
	}

	int get_int() {
		return proxy.get_int();
	}
};


class ArchTestSuite : public CxxTest::TestSuite
{
public:

	void test_network()
	{
		using std::unique_ptr;
		basic_network nw;

		Echo* srv { new Echo(&nw) };
		const size_t Ncli = 5;
		Echo_cli* cli[Ncli];
		for(size_t i=0;i<Ncli;i++) { 
			cli[i] = new Echo_cli(&nw);
			cli[i]->proxy <<= srv;
		};

		TS_ASSERT_EQUALS(nw.hosts().size(), Ncli+1);
		TS_ASSERT_EQUALS(nw.size(), Ncli+1);
		TS_ASSERT_EQUALS(nw.groups().size(), 0);
		TS_ASSERT_EQUALS(nw.channels().size(), 12*Ncli);

		size_t Echo_ifc = 1 << RPCC_BITS_PER_IFC;
		TS_ASSERT_EQUALS(nw.decl_interface(typeid(Echo)), Echo_ifc);

		TS_ASSERT_EQUALS(nw.rpc().get_interface(Echo_ifc).name(), string("Echo"));
		using namespace std::string_literals;
		string methods[7] = {
			"echo"s, "send_int"s, "get_int"s, "init"s,
			"say_bye"s, "finish"s, "add"s
		};
		for(size_t i=0; i<7; i++) {
			TS_ASSERT_EQUALS(nw.rpc().get_method(Echo_ifc| 2*(i+1)).name(), 
				methods[i]);
		}


		for(size_t i=0; i<Ncli; i++) {
			TS_ASSERT_EQUALS(cli[i]->proxy._r_ifc, 
										nw.decl_interface(typeid(Echo)));
			TS_ASSERT_EQUALS(cli[i]->proxy._r_owner, cli[i]);
			TS_ASSERT_EQUALS(cli[i]->proxy._r_proc, srv);
			TS_ASSERT_EQUALS(cli[i]->proxy._r_calls.size(), 7);
			for(size_t j=0;j<cli[i]->proxy._r_calls.size();j++) {
				TS_ASSERT_EQUALS(cli[i]->proxy._r_calls[j]->endpoint(), 
					Echo_ifc| 2*(j+1));
			}
		}

	}


	void test_rpc()
	{
		basic_network nw;

		Echo* srv = new Echo(&nw);
		Echo_cli* cli = new Echo_cli(&nw);

		TS_ASSERT_EQUALS(nw.channels().size(), 0);

		cli->proxy <<= srv;




		TS_ASSERT_EQUALS(cli->proxy._r_proc, srv);
		TS_ASSERT_EQUALS(cli->proxy._r_calls.size(), 7);

		TS_ASSERT_EQUALS( cli->send_echo("Hi"), "Echoing Hi" );
		TS_ASSERT_EQUALS( cli->proxy.echo.request_channel()->messages(), 1);
		TS_ASSERT_EQUALS( cli->proxy.send_int.request_channel()->messages(), 1);
		TS_ASSERT_EQUALS( cli->proxy.init.request_channel()->messages(), 1);

		TS_ASSERT_EQUALS( cli->get_int(), 10);


		cli->proxy.say_bye("bye");

		cli->proxy.finish();

	}

};

