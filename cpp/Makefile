#
# Makefile for the new DS simulator
#

#################################
# Compiler flags
#################################

ifndef DEBUG
# Default: compile for debug
DEBUG=1
endif


CXXFLAGS= -Wall 

DEBUGFLAGS=  -g3 
OPTFLAGS= -g3 -finline -march=native -O3 -DNDEBUG

ifeq ($(DEBUG),1)
CXXFLAGS+=  $(DEBUGFLAGS)
else
CXXFLAGS+=  $(OPTFLAGS)
endif

###################################
# File lists
###################################

DDS_SOURCES= agms.cc data_source.cc
TEST_SOURCES= unit_tests.cc data_tests.cc

CXX_SOURCES= $(DDS_SOURCES)

DDS_OBJ= $(DDS_SOURCES:.cc=.o)
CXX_OBJ= $(CXX_SOURCES:.cc=.o)

###################################
# Rules
###################################

.PHONY: all ddslib test clean distclean doc

all: ddslib $(TEST_SOURCES:.cc=) test

ddslib: libdds.a

libdds.a: $(DDS_OBJ)
	ar rcs $@ $^
	ranlib $@

unit_tests.cc: basic_tests.hh ams_tests.hh ds_tests.hh $(CXX_SOURCES)
	cxxtestgen --error-printer -o $@ $^

data_tests.cc: data_tests.hh $(CXX_SOURCES)
	cxxtestgen --error-printer -o $@ $^

unit_tests: unit_tests.o libdds.a
	$(CXX) $(CXXFLAGS) -o $@ $^ 

data_tests: data_tests.o libdds.a
	$(CXX) $(CXXFLAGS) -o $@ $^ 

test: unit_tests
	./unit_tests

data_test: data_tests
	./data_tests

clean:
	-rm $(CXX_OBJ) 
	-rm libdds.a 
	-rm $(TEST_SOURCES) $(TEST_SOURCES:.cc=.o) $(TEST_SOURCES:.cc=) 

depend: $(CXX_SOURCES) $(TEST_SOURCES)
	$(CXX) $(CXXFLAGS) -MM $^ > .depend

include .depend
