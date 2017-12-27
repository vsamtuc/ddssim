#ifndef __ECA_EVENT_HH__
#define __ECA_EVENT_HH__

#include "eca.hh"

namespace dds {

using eca::Event;
/**
	The system event types. The logic of the loop goes as follows
	(in context-free grammar notation)

	run -> INIT streamproc RESULTS DONE
	streamproc -> START_STREAM records END_STREAM
	records -> *empty*
	         | record records
	record -> START_RECORD VALIDATE REPORT END_RECORD

	Or, in loop logic:

	INIT;
	if(datasource exists and is valid) {
		START_STREAM;
		while(datasource is valid) {
			START_RECORD;
			VALIDATE;
			REPORT;
			END_RECORD;
			advance data source;
		}
		END_STREAM;
	}
	RESULTS;
	DONE;

	@{
 */

constexpr Event INIT(1);
constexpr Event DONE(2);

constexpr Event START_STREAM(3);
constexpr Event END_STREAM(4);

constexpr Event START_RECORD(5);
constexpr Event END_RECORD(6);

constexpr Event VALIDATE(7);
constexpr Event REPORT(8);

constexpr Event RESULTS(9);
/** @} */


/*
	Event types for various modules.
	Making these constexpr allows them to be
	used in switch statements, etc
*/


/// from accurate.hh
constexpr Event STREAM_SKETCH_UPDATED(100);
constexpr Event STREAM_SKETCH_INITIALIZED(101);



}




#endif