#
# Makefile for the new DS simulator
#


DDS_SOURCES= agms.cc data_source.cc
DDS_OBJ= $(DDS_SOURCES:.cc=.o)
TEST_SOURCES= basic_tests.cc
CXX_SOURCES= $(DDS_SOURCES)
CXX_OBJ= $(CXX_SOURCES:.cc=.o)

CXXFLAGS= -g -Wall 

.PHONY: all ddslib test clean distclean doc

all: ddslib basic_tests

ddslib: libdds.a

libdds.a: $(DDS_OBJ)
	ar rcs $@ $^
	ranlib $@

basic_tests.cc: basic_tests.hh ams_tests.hh ds_tests.hh $(CXX_SOURCES)
	cxxtestgen --error-printer -o $@ $^

basic_tests: basic_tests.o libdds.a
	$(CXX) $(CXXFLAGS) -o $@ $^ 

test: basic_tests
	./basic_tests


clean:
	-rm $(CXX_OBJ) 
	-rm libdds.a 
	-rm basic_tests.cc basic_tests basic_tests.o

depend: $(CXX_SOURCES) 
	$(CXX) $(CXXFLAGS) -MM $(CXX_SOURCES) > .depend

include .depend
