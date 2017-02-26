#include <cxxtest/TestSuite.h>

#include <string>
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

	ACK send_int(int i) { value=i; return ACK(); }
	int get_int() { return value; }

	ACK init() { return ACK(); }

	oneway say_bye(const string& msg) { value=-1; return oneway(); }
	oneway finish() { return oneway(); }

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
		basic_network nw;

		host* h1 = new host(&nw);
		host* h2 = new host(&nw);

		TS_ASSERT_EQUALS(nw.hosts().size(), 2);
		TS_ASSERT_EQUALS(nw.size(), 2);

	}	


	void test_rpc()
	{
		basic_network nw;

		Echo* srv = new Echo(&nw);
		Echo_cli* cli = new Echo_cli(&nw);
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

		print(nw.get_rpcc_name(cli->proxy.finish.endpoint()));

	}

};

