
// MyTestSuite2.h
#include <cxxtest/TestSuite.h>
#include <cstdio>

#include "agms.hh"

using std::all_of;

class AGMSTestSuite : public CxxTest::TestSuite
{
public:
    void testHashFamilyConstruction(void)
    {
    	agms_hash_family HF(5);

        TS_ASSERT_EQUALS( HF.depth(), 5);
    }


    void testHash()
    {
    	agms_hash_family HF(5);

		for(int i=0;i<1000;i++) {
			agms_hash_family::index_type h = HF.hash(i%5, 17*i+131);
			TS_ASSERT_LESS_THAN_EQUALS(0, h);
		}
    }

    void testFourwise()
    {
    	agms_hash_family HF(5);
		int count = 0;
		for(int i=0;i<1000;i++) {
			agms_hash_family::index_type h = HF.fourwise(i%5, 17*i+131);
			if(h & 1) count++;
		}    	
		TS_ASSERT_LESS_THAN_EQUALS(450, count);
		TS_ASSERT_LESS_THAN_EQUALS(count, 550);
    }


    void testCache() {
    	agms_hash_family* hf = agms_hash_family::get_cached(5);
    	TS_ASSERT_EQUALS(hf->depth(), 5);
    	TS_ASSERT_EQUALS(hf, agms_hash_family::get_cached(5));
    }


    void testConstructors() {
    	agms sk1;
    	TS_ASSERT(sk1.begin()==0);

    	agms sk2(5, 500);
    	TS_ASSERT_EQUALS(sk2.depth(), 5);
    	TS_ASSERT_EQUALS(sk2.width(), 500);
    	TS_ASSERT_EQUALS(sk2.size(), 2500);
    	TS_ASSERT(all_of(sk2.begin(), sk2.end(), 
    		[](double x) { return x==0.0; } ));

    	TS_ASSERT_EQUALS(sk2.hash_family(), 
    			agms_hash_family::get_cached(5));

    	agms sk3 = sk2;
    	TS_ASSERT_EQUALS(sk3.depth(), 5);
    	TS_ASSERT_EQUALS(sk3.width(), 500);
    	TS_ASSERT_EQUALS(sk3.size(), 2500);
    	TS_ASSERT(all_of(sk3.begin(), sk3.end(), 
    		[](double x) { return x==0.0; } ));

    	TS_ASSERT_EQUALS(sk3.hash_family(), 
    			agms_hash_family::get_cached(5));

    	agms sk4 = agms(7, 100);
    	TS_ASSERT_EQUALS(sk4.depth(), 7);
    	TS_ASSERT_EQUALS(sk4.width(), 100);
    	TS_ASSERT_EQUALS(sk4.size(), 700);
    	TS_ASSERT(all_of(sk4.begin(), sk4.end(), 
    		[](double x) { return x==0.0; } ));

    	TS_ASSERT_EQUALS(sk4.hash_family(), 
    			agms_hash_family::get_cached(7));
    }


	void testInitialized() {
		agms sk;
		TS_ASSERT(! sk.initialized());

		agms sk2(5, 1000);
		TS_ASSERT(sk2.initialized());
	}


	void testUpdate() {
		using key_type = agms::key_type;

		agms sk(5, 500);

		for(key_type k = 10; k< 1000; k+=17) 
			sk.insert(k);
		TS_ASSERT_DIFFERS(sk.norm_squared(), 0.0);

		for(key_type k = 10; k< 1000; k+=17) 
			sk.erase(k);
		TS_ASSERT_EQUALS(sk.norm_squared(), 0.0);
	}


	void testAssignConst() {
		agms sk(5, 500);

		sk = 3.0;
		for(auto x = sk.begin(); x!=sk.end(); x++ )
			TS_ASSERT_EQUALS(*x, 3.0);

		sk *= 5;
		for(auto x = sk.begin(); x!=sk.end(); x++ )
			TS_ASSERT_EQUALS(*x, 15.0);

		sk = 0;
		for(auto x = sk.begin(); x!=sk.end(); x++ )
			TS_ASSERT_EQUALS(*x, 0.0);
		TS_ASSERT_EQUALS(sk.norm_squared(), 0.0);
	}

	void testAdd() {
		agms sk1(5, 500);
		agms sk2 = sk1;

		sk1 = 3.0;
		sk2 = 12;

		agms sk = sk1+sk2;

		for(auto x = sk.begin(); x!=sk.end(); x++ )
			TS_ASSERT_EQUALS(*x, 15.0);
	}



};
