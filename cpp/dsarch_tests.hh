
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
		auto ret = proxy.send_int(10);
		TS_ASSERT( ret );
		TS_ASSERT_EQUALS( ret.payload , 11);
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
		TS_ASSERT_EQUALS(nw.groups().size(), 1);

		size_t Echo_ifc = 1 << RPCC_BITS_PER_IFC;
		TS_ASSERT_EQUALS(nw.decl_interface(typeid(Echo)), Echo_ifc);

		TS_ASSERT_EQUALS(nw.rpc().get_interface(Echo_ifc).num_channels(), 12);
		TS_ASSERT_EQUALS(nw.channels().size(), 12*Ncli);

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


	void test_rpc_channels()
	{
		basic_network nw;

		Echo* srv = new Echo(&nw);
		Echo_cli* cli = new Echo_cli(&nw);

		// no channels yet
		TS_ASSERT_EQUALS(nw.channels().size(), 0);

		// creates channels
		cli->proxy <<= srv;

		chan_frame chan(nw);

		TS_ASSERT_EQUALS(chan.size(), 
			nw.rpc().get_interface(nw.rpc().code(typeid(Echo))).num_channels()
			);

		TS_ASSERT_EQUALS(chan.count(), 0);
		TS_ASSERT_EQUALS(chan.sum(), 0);
		TS_ASSERT_EQUALS(chan.src(srv).size(), 5);
		TS_ASSERT_EQUALS(chan.dst(srv).size(), 7);

		TS_ASSERT_EQUALS(chan.endp_req().size(), 7);
		TS_ASSERT_EQUALS(chan.endp_rsp().size(), 5);

		TS_ASSERT_EQUALS(cli->proxy._r_proc, srv);
		TS_ASSERT_EQUALS(cli->proxy._r_calls.size(), 7);

		TS_ASSERT_EQUALS( cli->send_echo("Hi"), "Echoing Hi" );
		// The above call executed
		//  srv-> init()      returning NAK
		//  srv-> send_int(10) returning Acknoledge<int>(11)
		//  srv-> echo("Hi")  returning "Echoing Hi"

		TS_ASSERT_EQUALS(chan.src(srv).count(), 2);
		TS_ASSERT_EQUALS(chan.dst(srv).count(), 3);
		TS_ASSERT_EQUALS(chan.count(), 5);

		TS_ASSERT_EQUALS(chan.src(srv).sum(), sizeof(int)+10);
		TS_ASSERT_EQUALS(chan.dst(srv).sum(), sizeof(int)+2);
		TS_ASSERT_EQUALS(chan.sum(), 2*sizeof(int) + 12);


		TS_ASSERT_EQUALS( cli->get_int(), 10);
		TS_ASSERT( cli->proxy.send_int(10) );     // returns Ack(11)
		TS_ASSERT( !cli->proxy.send_int(-10) );   // returns NAK
		TS_ASSERT_EQUALS( cli->get_int(), -10);
		cli->proxy.say_bye("bye");
		cli->proxy.finish();

		TS_ASSERT_EQUALS(chan.endp_req().count(), 9);
		TS_ASSERT_EQUALS(chan.endp_rsp().count(), 5);
	}

};

