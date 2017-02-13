#include <cxxtest/TestSuite.h>

#include <string>
#include "dsarch.hh"

using namespace dds;
using std::string;


struct Echo : process
{
	int value;

	Echo(network* nw) : process(nw), value(0) {}

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

	Echo_cli(network* nw) 
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
		network nw;

		host* h1 = new host(&nw);
		host* h2 = new host(&nw);
		channel* c12  = nw.connect(h1, h2);

		TS_ASSERT_EQUALS(nw.hosts().size(), 2);
		TS_ASSERT_EQUALS(nw.size(), 2);

		TS_ASSERT_EQUALS(c12, h1->channel_to(h2));
		TS_ASSERT_EQUALS(nullptr, h2->channel_to(h1));
		TS_ASSERT_EQUALS( nw.connect(h1,h2), c12 );
	}	


	void test_rpc()
	{
		network nw;

		Echo* srv = new Echo(&nw);
		Echo_cli* cli = new Echo_cli(&nw);
		cli->proxy.connect(srv);
		auto c12 = cli->proxy.request_channel();
		auto c21 = cli->proxy.response_channel();

		TS_ASSERT( cli->proxy.response_channel() );

		TS_ASSERT_EQUALS( cli->send_echo("Hi"), "Echoing Hi" );
		TS_ASSERT_EQUALS(c12->messages(), 3);
		TS_ASSERT_EQUALS(c12->bytes(), 2+sizeof(int));
		TS_ASSERT_EQUALS(c21->messages(), 3);
		TS_ASSERT_EQUALS(c21->bytes(), 10);

		TS_ASSERT_EQUALS( cli->get_int(), 10);

		TS_ASSERT_EQUALS(c12->messages(), 4);
		TS_ASSERT_EQUALS(c12->bytes(), 2+sizeof(int));
		TS_ASSERT_EQUALS(c21->messages(), 4);
		TS_ASSERT_EQUALS(c21->bytes(), 14);

		cli->proxy.say_bye("bye");
		TS_ASSERT_EQUALS(c12->messages(), 5);
		TS_ASSERT_EQUALS(c12->bytes(), 2+sizeof(int)+3);
		TS_ASSERT_EQUALS(c21->messages(), 4);
		TS_ASSERT_EQUALS(c21->bytes(), 14);

		cli->proxy.finish();
		TS_ASSERT_EQUALS(c12->messages(), 6);
		TS_ASSERT_EQUALS(c12->bytes(), 2+sizeof(int)+3);
		TS_ASSERT_EQUALS(c21->messages(), 4);
		TS_ASSERT_EQUALS(c21->bytes(), 14);

	}

};

