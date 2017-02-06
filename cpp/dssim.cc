
#include <cstdio>
#include <chrono>
#include <libconfig.h++>

#include <typeinfo>

#include "data_source.hh"
#include "method.hh"
#include "accurate.hh"
#include "output.hh"

using namespace dds;


void execute()
{
	/* Set up data stream */

	//data_source* wcup = crawdad_ds("/home/vsam/src/datasets/wifi_crawdad_sorted");
	data_source* wcup = wcup_ds("/home/vsam/src/datasets/wc_day44");

	dataset D;
	D.load(wcup);
	//D.set_max_length(100000);
	D.hash_sources(4);
	//D.hash_streams(1);
	D.set_time_window(3600);
	D.create();

	/* Create components */

	std::vector<reactive*> components;
	std::vector<stream_id> sids;
	std::copy(CTX.metadata().stream_ids().begin(),
		CTX.metadata().stream_ids().end(),
		back_inserter(sids));

	cout << "Treating " << sids.size() << " streams" << endl;

	for(size_t i=0; i<sids.size(); i++) {
		cout << "Treating stream " << i << endl;
		components.push_back(new selfjoin_exact_method(sids[i]));
		components.push_back(new selfjoin_agms_method(sids[i], 17, 50000));
		for(size_t j=i; j>0; j--){
		//	components.push_back(new twoway_join_exact_method(sids[j-1],sids[i]));			
		}
	}

	//data_source_statistics stat;

	/* Create output files */

	output_file* sto = CTX.open(stdout);
	output_file* wcout = 
		CTX.open("wc_tseries.dat",open_mode::truncate);
	
	/* Bind files to outputs */

	//CTX.timeseries.bind(sto);
	CTX.timeseries.bind(wcout);

	/* Configure the timeseries reporting */
	reporter repter(CTX.metadata().size()/1000);

	/* Print a progress bar */
	progress_reporter pbar(stdout, 40, "Progress: ");

	/* Run */
	using namespace std::chrono;
	steady_clock::time_point startt = steady_clock::now();
	CTX.run();
	steady_clock::time_point endt = steady_clock::now();
	cout << "Execution time=" 
		<< duration_cast<milliseconds>(endt-startt).count()/1000.0
		<< "sec" << endl;

	/* Clean up */
	for(auto p : components)
		delete p;
	CTX.close_result_files();
}


void execute_generated()
{
	/* Set up data stream */

	auto ds = new uniform_data_source(5, 25, 1000000, 100000);
	CTX.data_feed(ds);


	/* Create components */

	std::vector<reactive*> components;
	std::vector<stream_id> sids;
	std::copy(CTX.metadata().stream_ids().begin(),
		CTX.metadata().stream_ids().end(),
		back_inserter(sids));

	cout << "Treating " << sids.size() << " streams" << endl;

	for(size_t i=0; i<sids.size(); i++) {
		cout << "Treating stream " << i << endl;
		components.push_back(new selfjoin_exact_method(sids[i]));
		components.push_back(new selfjoin_agms_method(sids[i], 7, 1000));
		for(size_t j=i; j>0; j--)
			components.push_back(new twoway_join_exact_method(sids[j-1],sids[i]));
	}

	data_source_statistics stat;

	/* Create output files */

	output_file* sto = CTX.open(stdout);
	output_file* wcout = 
	CTX.open("uni_tseries.dat",open_mode::truncate);
	
	/* Bind files to outputs */

	CTX.timeseries.bind(sto);
	CTX.timeseries.bind(wcout);

	/* Configure the timeseries reporting */
	reporter repter(10000);

	/* Print a progress bar */
	progress_reporter pbar(stdout, 40, "Progress: ");

	/* Run */
	using namespace std::chrono;
	steady_clock::time_point startt = steady_clock::now();
	CTX.run();
	steady_clock::time_point endt = steady_clock::now();
	cout << "Execution time=" 
		<< duration_cast<milliseconds>(endt-startt).count()/1000.0
		<< "sec" << endl;

	/* Clean up */
	for(auto p : components)
		delete p;
	CTX.close_result_files();	
}



using libconfig::Config;

Config cfg;



int main(int argc, char** argv)
{
	if(argc>1) {
		cfg.readFile(argv[1]);
	}


	execute();
	//execute_generated();
	return 0;
}



