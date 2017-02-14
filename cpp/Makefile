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


CXXFLAGS= -Wall -fPIC -I/usr/include/python3.5m -std=gnu++17

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

DDS_SOURCES= mathlib.cc output.cc eca.cc agms.cc data_source.cc method.cc \
	accurate.cc dds.cc dsarch.cc results.cc\
	tods.cc

UNIT_TESTS= basic_tests.hh ams_tests.hh ds_tests.hh dsarch_tests.hh \
	mathlib_tests.hh exec_tests.hh output_tests.hh \
	tods_tests.hh

TEST_SOURCES= unit_tests.cc data_tests.cc

CXX_SOURCES= $(DDS_SOURCES) dssim.cc dds_module.cc

DDS_OBJ= $(DDS_SOURCES:.cc=.o)
CXX_OBJ= $(CXX_SOURCES:.cc=.o)

###################################
# Rules
###################################

.PHONY: all ddslib test clean distclean doc pyapi

all: ddslib pyapi dssim $(TEST_SOURCES:.cc=) test

ddslib: libdds.a 

pyapi: _dds.so

libdds.a: $(DDS_OBJ)
	ar rcs $@ $^
	ranlib $@

dssim: dssim.o libdds.a
	$(CXX) -o $@ $< -L. -ldds -lconfig++

dds_module.o: dds_module.cc
	$(CXX) -Wno-deprecated $(CXXFLAGS) -o $@ -c $< 

_dds.so: dds_module.o libdds.a
	$(CXX) -shared -o $@ $^ -lboost_python-py35 -lpython3.5dm -lpthread -ldl  -lutil -lm

#unit_tests.cc: $(UNIT_TESTS) 
#	cxxtestgen --error-printer -o $@ $^
#unit_tests.o: unit_tests.cc

#data_tests.cc: data_tests.hh 
#	cxxtestgen --error-printer -o $@ $^

#unit_tests: unit_tests.o libdds.a
unit_tests.cc:
	cxxtestgen --root --error-printer -o $@ $<	

unit_tests: $(UNIT_TESTS:.hh=.o) unit_tests.o libdds.a
	$(CXX) $(CXXFLAGS) -o $@ $^ 

data_tests.cc: data_tests.hh
	cxxtestgen --error-printer -o $@ $<	

data_tests: data_tests.o libdds.a
	$(CXX) $(CXXFLAGS) -o $@ $^ 

%_tests.cc : %_tests.hh
	cxxtestgen --part --error-printer -o $@ $<

test: unit_tests
	./unit_tests

data_test: data_tests
	./data_tests

doc:
	doxygen dds.cfg

clean:
	-rm $(CXX_OBJ) 
	-rm libdds.a dssim dds.so dds_module.o
	-rm $(UNIT_TESTS:.hh=.cc) $(UNIT_TESTS:.hh=.o)
	-rm $(TEST_SOURCES) $(TEST_SOURCES:.cc=.o) $(TEST_SOURCES:.cc=) 

depend: $(CXX_SOURCES) $(TEST_SOURCES) $(UNIT_TESTS:.hh=.cc)
	$(CXX) $(CXXFLAGS) -MM $^ > .depend

include .depend
