
#include <cstdio>
#include <cstdlib>
#include <chrono>

#include <typeinfo>

#include "data_source.hh"
#include "method.hh"
#include "accurate.hh"
#include "output.hh"
#include "results.hh"
#include "tods.hh"
#include "agm.hh"
#include "gm.hh"

using namespace dds;


void execute()
{
	/* Set up data stream */
	string HOME(getenv("HOME"));

	//datasrc wcup = crawdad_ds(HOME+"/src/datasets/wifi_crawdad_sorted");
	//datasrc wcup = wcup_ds(HOME+"/src/datasets/wc_day44");
	//datasrc wcup = wcup_ds(HOME+"/src/datasets/wc_day44_1");
	datasrc wcup = wcup_ds(HOME+"/src/datasets/wc_day46");
	//datasrc wcup = hdf5_ds(HOME+"/git/ddssim/cpp/wc_test.h5","dsstream");

	dataset D;
	D.load(wcup);
	//D.set_max_length(10000);
	//D.hash_sources(4);
	D.hash_streams(1);
	D.set_time_window(2*3600);
	D.warmup_time(2*3600, true);
	D.create();


	/* Create components */

	std::vector<reactive*> components;
	std::vector<stream_id> sids;
	std::copy(CTX.metadata().stream_ids().begin(),
		CTX.metadata().stream_ids().end(),
		back_inserter(sids));

	cout << "Treating " << sids.size() << " streams" << endl;

	projection proj(7 , 1500);
	proj.set_epsilon( 0.05 );

	for(size_t i=0; i<sids.size(); i++) {
		// cout << "Treating stream " << sids[i] << endl;
		// components.push_back(new selfjoin_exact_method(sids[i]));
		// components.push_back(new selfjoin_agms_method(sids[i], proj));
		// for(size_t j=i; j>0; j--){
		// 	components.push_back(new twoway_join_exact_method(sids[j-1],sids[i]));			
		// 	components.push_back(new 
		// 		twoway_join_agms_method(sids[j-1], sids[i], 15, 10000));
		// }
	}
	//components.push_back(new tods::network(proj, 0.025 ));
	components.push_back(new agm::network(0, proj, 0.1 ));
	//components.push_back(new gm::network(0, proj, 0.1 ));

	/* Create output files */
	reporter R;

	output_file* sto = CTX.open(stdout);
	
	/* Bind files to outputs */


	// output_file* lsstats_file = CTX.open("wc_lsstats.dat");
	// auto lss = new data_source_statistics();
	// components.push_back(lss);
	// local_stream_stats.bind(lsstats_file);
	// R.watch(local_stream_stats);

	output_hdf5 h5f("wc_results.h5");

	network_comm_results.bind(sto);
	network_comm_results.bind(&h5f);
	R.watch(network_comm_results);

	network_host_traffic.bind(&h5f);
	network_host_traffic.bind(sto);
	R.watch(network_host_traffic);

	network_interfaces.bind(&h5f);
	R.watch(network_interfaces);

	/* Configure the timeseries reporting */
	output_file* wcout = 
		CTX.open("wc_tseries.dat",open_mode::truncate);
	//CTX.timeseries.bind(sto);
	CTX.timeseries.bind(wcout);
	CTX.timeseries.bind(&h5f);
	R.emit_row(CTX.timeseries, n_times(1000));

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
	agms_sketch_updater_factory.clear();
}


void execute_generated()
{
	/* Set up data stream */

	auto ds = uniform_datasrc(5, 25, 10000, 1000);
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
	components.push_back(new tods::network(7, 10000, 0.05));

	data_source_statistics stat;

	/* Create output files */
	reporter R;

	output_file* sto = CTX.open(stdout);
	output_file* wcout = 
	CTX.open("uni_tseries.dat",open_mode::truncate);
	
	/* Bind files to outputs */

	CTX.timeseries.bind(sto);
	CTX.timeseries.bind(wcout);

	/* Configure the timeseries reporting */
	R.emit_row(CTX.timeseries, every_n_times(CTX.metadata().size()/1000));

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




int main(int argc, char** argv)
{

	execute();
	//execute_generated();
	return 0;
}



