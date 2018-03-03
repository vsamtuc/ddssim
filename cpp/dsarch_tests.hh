
#include <memory>
#include <string>
#include <boost/range/adaptors.hpp>

#include <cxxtest/TestSuite.h>
#include "dsarch.hh"
#include "binc.hh"

using namespace dsarch;
using std::string;
using binc::print;

/****************************************
	A simple Echo client-server network
*****************************************/

struct Echo;
struct Echo_cli;

struct Echo_network : basic_network
{
	Echo_network() 
	{}
};



struct Echo : process
{
	Echo_network* nw;
	int value;

	Echo(Echo_network* _nw) 
		: process(_nw), nw(_nw), value(0)
	{
	}

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

/****************************************
	A p2p scatter-gather network 

	Each peer has a key (an integer).
	Periodically, when the ksy changes,
	a peer asks the network for the number
	of peers with equal key.

	The peer has two proxies: a unicast and 
	mutlicast.

	The multicast group is used to send out 
	the inquiry, when a key changes.
	The unicast group is used to reply to
	the sender of an inquiry.
*****************************************/

struct PeerNetwork;
struct Peer;

struct PeerNetwork : basic_network
{
	mcast_group<Peer> peers; // this does not need a full type!
	PeerNetwork() : basic_network(), peers(this) { }
};

struct Peer_proxy;
struct Peer_mcast_proxy;

struct Peer : process
{
	PeerNetwork* p2pnet;

	// note how the proxy map to this class is
	// definable, even though the class is not
	// defined yet!
	//
	// The only problem is that the
	// constructor and methods have to be defined outside the
	// class
	proxy_map<Peer_proxy, Peer> peermap;

	// Local state
	int key;
	int arity;

	Peer(PeerNetwork* nw, int k) 
	: process(nw), p2pnet(nw), peermap(this),
	  key(k), arity(1)
	{ }


	// Remote methods
	oneway scatter_inquiry(sender<Peer> sender, int x);
	oneway gather_replies(sender<Peer> replier, int x);

	// This is called externally
	void change_key(int newkey);
};

//
//  The proxy
//

struct Peer_proxy : remote_proxy<Peer>
{
	REMOTE_METHOD(Peer, gather_replies);
	REMOTE_METHOD(Peer, scatter_inquiry);
	Peer_proxy(process* owner) : remote_proxy<Peer>(owner) {}
};


//
//  Peer methods
//


oneway Peer::scatter_inquiry(sender<Peer> who, int what) 
{
	if(key==what) {
		if(who.value!=this)
			// send reply to other peers, via proxies
			peermap[who.value].gather_replies(this, key);
		else
			// send to myself, but not via a proxy
			gather_replies(this, key);
	}
}

oneway Peer::gather_replies(sender<Peer> who, int x)
{
	if(x==key)
		arity++;
}

void Peer::change_key(int newkey)
{
	key = newkey;
	arity = 0;
	// broadcast an inquiry
	peermap[p2pnet->peers].scatter_inquiry(this, key);
	// the gather messages will have computed the arity by now
}



//
//  Test suite
//

class ArchTestSuite : public CxxTest::TestSuite
{
public:

	void test_network()
	{
		Echo_network nw;

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
		TS_ASSERT_EQUALS(nw.channels().size(), 12*Ncli );

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

		
		for(size_t i=0;i<Ncli;i++) { 
			delete cli[i];
		}
		delete srv;
	}


	void test_rpc_channels()
	{
		Echo_network nw;

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

		TS_ASSERT_EQUALS(chan.msgs(), 0);
		TS_ASSERT_EQUALS(chan.bytes(), 0);
		TS_ASSERT_EQUALS(chan.src(srv).size(), 5 );
		TS_ASSERT_EQUALS(chan.dst(srv).size(), 7);

		TS_ASSERT_EQUALS(chan.endp_req().size(), 7 );
		TS_ASSERT_EQUALS(chan.endp_rsp().size(), 5);

		TS_ASSERT_EQUALS(cli->proxy._r_proc, srv);
		TS_ASSERT_EQUALS(cli->proxy._r_calls.size(), 7);

		TS_ASSERT_EQUALS( cli->send_echo("Hi"), "Echoing Hi" );
		// The above call executed
		//  srv-> init()      returning NAK
		//  srv-> send_int(10) returning Acknoledge<int>(11)
		//  srv-> echo("Hi")  returning "Echoing Hi"

		TS_ASSERT_EQUALS(chan.src(srv).msgs(), 2);
		TS_ASSERT_EQUALS(chan.dst(srv).msgs(), 3);
		TS_ASSERT_EQUALS(chan.msgs(), 5);

		TS_ASSERT_EQUALS(chan.src(srv).bytes(), sizeof(int)+10);
		TS_ASSERT_EQUALS(chan.dst(srv).bytes(), sizeof(int)+2);
		TS_ASSERT_EQUALS(chan.bytes(), 2*sizeof(int) + 12);


		TS_ASSERT_EQUALS( cli->get_int(), 10);
		TS_ASSERT( cli->proxy.send_int(10) );     // returns Ack(11)
		TS_ASSERT( !cli->proxy.send_int(-10) );   // returns NAK
		TS_ASSERT_EQUALS( cli->get_int(), -10);
		cli->proxy.say_bye("bye");
		cli->proxy.finish();

		TS_ASSERT_EQUALS(chan.endp_req().msgs(), 9);
		TS_ASSERT_EQUALS(chan.endp_rsp().msgs(), 5);

		delete cli;
		delete srv;
	}



	void test_multicast()
	{
		PeerNetwork p2p;

		// Create the network with 4 peers
		vector<Peer*> P;
		for(int i=0; i<4; i++) {
			auto p = new Peer(&p2p, i);
			p->set_addr(i);
			P.push_back(p);
			p2p.peers.join(p);
		}

		P[0]->change_key(1);
		TS_ASSERT_EQUALS(P[0]->arity, 2);
		TS_ASSERT_EQUALS(P[1]->arity, 1);
		TS_ASSERT_EQUALS(P[2]->arity, 1);
		TS_ASSERT_EQUALS(P[3]->arity, 1);

		chan_frame cf(&p2p);

		// There are only 4 channels, two for each of the
		// constructed proxies: a multicast proxy 0->-2  and a 
		// unicast proxy 1->0
		TS_ASSERT_EQUALS(cf.size(), 4);
		TS_ASSERT_EQUALS(cf.unicast().msgs(), 1);
		TS_ASSERT_EQUALS(cf.multicast().msgs(), 1);

		P[0]->change_key(2);
		P[1]->change_key(2);
		P[2]->change_key(2);
		TS_ASSERT_EQUALS(P[0]->arity, 2);
		TS_ASSERT_EQUALS(P[1]->arity, 3);
		TS_ASSERT_EQUALS(P[2]->arity, 3);
		TS_ASSERT_EQUALS(P[3]->arity, 1);

		cf = chan_frame(&p2p);
		TS_ASSERT_EQUALS(cf.unicast().msgs(), 6);
		TS_ASSERT_EQUALS(cf.multicast().msgs(), 4);
		TS_ASSERT_EQUALS(cf.multicast().recv_msgs(), 12);

		for(auto&& p : P)
			delete p;
	}



};

