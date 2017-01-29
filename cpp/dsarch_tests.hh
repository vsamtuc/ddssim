#include <cxxtest/TestSuite.h>

#include <string>
#include "dsarch.hh"

using namespace dds;
using std::string;


struct Echo : process
{
	int value;

	Echo(network* nw) : process(nw), value(0) {}

	string echo(const string& msg) {
		return string("Echoing ")+msg;
	}

	void send_int(int i) { value=i; }
	int get_int() { return value; }

	void init() { }

	void say_bye(const string& msg) { value=-1; }
	void finish() { }
};

struct Echo_proxy : remote_proxy<Echo>
{
	remote_method<Echo, const string&, string> echo;
	remote_method<Echo, int, void> send_int;
	remote_method<Echo, void, int> get_int;
	remote_method<Echo, void, void> init;
	remote_method<Echo, const string&, oneway> say_bye;
	remote_method<Echo, void, oneway> finish;

	Echo_proxy(process* owner, Echo* obj) 
	: remote_proxy<Echo>(owner, obj),
	  echo(this, &Echo::echo),
	  send_int(this, &Echo::send_int),
	  get_int(this, &Echo::get_int),
	  init(this, &Echo::init),
	  say_bye(this, &Echo::say_bye),
	  finish(this, &Echo::finish)
	{}
};

struct Echo_cli : process
{
	Echo_proxy proxy;

	Echo_cli(network* nw, Echo* srv) 
	: process(nw), proxy(this, srv)
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
		Echo_cli* cli = new Echo_cli(&nw, srv);
		auto c12 = cli->proxy.request_channel();
		auto c21 = cli->proxy.response_channel();

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

