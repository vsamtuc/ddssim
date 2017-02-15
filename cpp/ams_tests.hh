
// MyTestSuite2.h
#include <cxxtest/TestSuite.h>
#include <cstdio>

#include "agms.hh"

using std::all_of;
using std::begin;
using std::end;

using namespace agms;


class AGMSTestSuite : public CxxTest::TestSuite
{
public:
    void testHashFamilyConstruction(void)
    {
    	hash_family HF(5);

        TS_ASSERT_EQUALS( HF.depth(), 5);
    }


    void testHash()
    {
    	hash_family HF(5);

		for(int i=0;i<1000;i++) {
			index_type h = HF.hash(i%5, 17*i+131);
			TS_ASSERT_LESS_THAN_EQUALS(0, h);
		}
    }

    void testFourwise()
    {
    	hash_family HF(5);
		int count = 0;
		for(int i=0;i<1000;i++) {
			index_type h = HF.fourwise(i%5, 17*i+131);
			if(h & 1) count++;
		}    	
		TS_ASSERT_LESS_THAN_EQUALS(450, count);
		TS_ASSERT_LESS_THAN_EQUALS(count, 550);
    }


    void testCache() {
    	hash_family* hf = hash_family::get_cached(5);
    	TS_ASSERT_EQUALS(hf->depth(), 5);
    	TS_ASSERT_EQUALS(hf, hash_family::get_cached(5));
    }


    void testConstructors() {
    	sketch sk1;

    	sketch sk2(5, 500);
    	TS_ASSERT_EQUALS(sk2.depth(), 5);
    	TS_ASSERT_EQUALS(sk2.width(), 500);
    	TS_ASSERT_EQUALS(sk2.size(), 2500);
    	TS_ASSERT(all_of(begin(sk2), end(sk2), 
    		[](double x) { return x==0.0; } ));

    	TS_ASSERT_EQUALS(sk2.hashf(), 
    			hash_family::get_cached(5));

    	sketch sk3 = sk2;
    	TS_ASSERT_EQUALS(sk3.depth(), 5);
    	TS_ASSERT_EQUALS(sk3.width(), 500);
    	TS_ASSERT_EQUALS(sk3.size(), 2500);
    	TS_ASSERT(all_of(begin(sk3), end(sk3), 
    		[](double x) { return x==0.0; } ));

    	TS_ASSERT_EQUALS(sk3.proj.hashf(), 
    			hash_family::get_cached(5));

    	sketch sk4 = sketch(7, 100);
    	TS_ASSERT_EQUALS(sk4.depth(), 7);
    	TS_ASSERT_EQUALS(sk4.width(), 100);
    	TS_ASSERT_EQUALS(sk4.size(), 700);
    	TS_ASSERT(all_of(begin(sk4), end(sk4), 
    		[](double x) { return x==0.0; } ));

    	TS_ASSERT_EQUALS(sk4.hashf(), 
    			hash_family::get_cached(7));
    }


    void test_assign()
    {
    	sketch sk(2,2);
    	sk = sketch(1,2);
    	TS_ASSERT_EQUALS(sk.depth(),1);
    	TS_ASSERT_EQUALS(sk.width(),2);
    	TS_ASSERT_EQUALS(sk.size(), 2);

    	TS_ASSERT_THROWS( (sk=Vec{1,2,3}) , std::length_error);
    	TS_ASSERT_THROWS_NOTHING( (sk=Vec{1,2}) );

    }


	void testUpdate() {
		sketch sk(5, 500);

		for(key_type k = 10; k< 1000; k+=17) 
			sk.insert(k);
		TS_ASSERT_DIFFERS(sk.norm2_squared(), 0.0);

		for(key_type k = 10; k< 1000; k+=17) 
			sk.erase(k);
		TS_ASSERT_EQUALS(sk.norm2_squared(), 0.0);
		TS_ASSERT(sk.size()==2500);
	}



	void testAssignConst() {
		sketch sk(5, 500);

		sk = 3.0;
		for(auto x = begin(sk); x!=end(sk); x++ )
			TS_ASSERT_EQUALS(*x, 3.0);

		sk *= 5;
		for(auto x = begin(sk); x!=end(sk); x++ )
			TS_ASSERT_EQUALS(*x, 15.0);

		sk = 0;
		for(auto x = begin(sk); x!=end(sk); x++ )
			TS_ASSERT_EQUALS(*x, 0.0);
		TS_ASSERT_EQUALS(sk.norm2_squared(), 0.0);
	}

	void test_vecops() {
		sketch sk1(5, 500);
		sketch sk2 = sk1;

		sk1 = 3.0;
		sk2 = 12;

		sketch sk = sk1+sk2;
		TS_ASSERT_EQUALS(sk.width(), 500);
		TS_ASSERT_EQUALS(sk.depth(), 5);
		for(auto x = begin(sk); x!=end(sk); x++ )
			TS_ASSERT_EQUALS(*x, 15.0);

		sketch sk3 = 3.0*sk1;
		TS_ASSERT_EQUALS(sk3.proj, sk1.proj);
		TS_ASSERT( (sk3==9.0).min() );

		sketch sk4 = 4.0*sk1;
		TS_ASSERT_EQUALS(sk4.proj, sk1.proj);
		TS_ASSERT( ((sk4-sk3)==3.0).min() );

		TS_ASSERT( ((sk4/2.0)==6.0).min() );
		TS_ASSERT( (sk4/2.0).proj == sk1.proj );
	}


	void test_incremental()
	{
		//
		// Try on two "streams"
		//

		projection proj = projection(7,1000);
		isketch isk[2] = { 
			isketch(proj), isketch(proj) };
		incremental_norm2  SJ[2] = { & isk[0], & isk[1] };
		incremental_prod P(&isk[0], &isk[1]);

		int s=0;
		for(key_type i = 1; i<100000; i++) {
			key_type key = i*i + 13*i +7;

			isk[s].update(key);

			SJ[s].update_incremental();

			if(s==0)
				P.update_incremental_1();
			else
				P.update_incremental_2();

			// switch sketches next round
			s = 1-s;
		}

		// Check against precise
		Vec incr[2] = { SJ[0].row_est, SJ[1].row_est  };
		SJ[0].update_directly();
		SJ[1].update_directly();

		for(size_t k=0; k<2; k++) {
			Vec dif = SJ[k].row_est - incr[k];

			dif *= dif;
			double err_num = sqrt(dif.sum());
			double err_denom = sqrt(pow(SJ[k].row_est,2.0).sum());
			double err = err_num/err_denom;	
			TS_ASSERT( err <= 1E-9 );
		}

		Vec incrP = P.row_est;
		P.update_directly();
		Vec dif = P.row_est - incrP;
		dif *= dif;
		double err_num = dif.sum();
		double err_denom = sqrt(pow(P.row_est,2.0).sum());
		double err = err_num/err_denom;
		TS_ASSERT( err <= 1E-9 );

	}


};
