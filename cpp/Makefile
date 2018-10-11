#
# Makefile for the new DS simulator
#

#################################
# Compiler flags
#################################

ifndef MODE
MODE=DEBUG
endif

PYTHON_INCLUDE:= $(shell /usr/bin/python3-config --includes)
PYTHON_LIB:= $(shell /usr/bin/python3-config --ldflags)

HDF5_INCLUDE:= -I/usr/include/hdf5/serial
HDF5_LIB:= -L/usr/lib/x86_64-linux-gnu/hdf5/serial -lhdf5_hl_cpp -lhdf5_cpp -lhdf5_hl \
	-lhdf5_serial -ldl -laec -lsz -lz

INCLUDE= $(PYTHON_INCLUDE) $(HDF5_INCLUDE)
LIB=  $(HDF5_LIB) -lm -ljsoncpp  #-lboost_filesystem -lboost_system
CXXFLAGS= -Wall -std=gnu++17 $(INCLUDE) # -fPIC

DEBUG_FLAGS=  -g3
OPT_FLAGS= -g3 -Ofast #-DNDEBUG
PRODUCTION_FLAGS = -Ofast -DNDEBUG

ifeq ($(MODE),DEBUG)
CXXFLAGS+=  $(DEBUG_FLAGS)
else ifeq ($(MODE),OPT)
CXXFLAGS+= $(OPT_FLAGS)
else ifeq ($(MODE),PRODUCTION)
CXXFLAGS+= $(PRODUCTION_FLAGS)
else
$(error Unknown mode $(MODE))
endif

ifdef GPROF
CXXFLAGS += -pg -no-pie
LDFLAGS += -pg  -no-pie
endif


###################################
# File lists
###################################

DDS_SOURCES= hdv.cc dds.cc output.cc eca.cc agms.cc data_source.cc method.cc \
	cfgfile.cc dsarch.cc \
	accurate.cc query.cc results.cc\
	sz_quorum.cc sz_bilinear.cc\
	tods.cc  safezone.cc gm_proto.cc gm_szone.cc gm_query.cc fgm.cc sgm.cc frgm.cc

UNIT_TESTS= basic_tests.hh ams_tests.hh ds_tests.hh dsarch_tests.hh \
	hdv_tests.hh exec_tests.hh output_tests.hh \
	tods_tests.hh szone_tests.hh method_tests.hh \
	cfg_tests.hh gm_tests.hh

TEST_SOURCES= unit_tests.cc data_tests.cc timing_tests.cc

CXX_SOURCES= $(DDS_SOURCES) dssim.cc dds_module.cc

DDS_OBJ= $(DDS_SOURCES:.cc=.o)
CXX_OBJ= $(CXX_SOURCES:.cc=.o)

###################################
# Rules
###################################

.PRECIOUS: unit_tests
.PHONY: all ddslib test clean distclean doc pyapi

all: ddslib dssim $(TEST_SOURCES:.cc=) test #pyapi

ddslib: libdds.a

pyapi: _dds.so

libdds.a: $(DDS_OBJ)
	ar rcs $@ $^
	ranlib $@

dssim: dssim.o libdds.a
	$(CXX) $(LDFLAGS) -o $@ $< -L. -ldds $(LIB)

dds_module.o: dds_module.cc
	$(CXX) -Wno-deprecated $(CXXFLAGS) -o $@ -c $<

_dds.so: dds_module.o libdds.a
	#$(CXX) -shared -o $@ $^ -lboost_python-py35 -lpython3.5dm -lpthread -ldl -lutil $(LIB)
	$(CXX) -shared -o $@ $^ -lboost_python-py35 $(LIB) $(PYTHON_LIB)

#unit_tests.cc: $(UNIT_TESTS)
#	cxxtestgen --error-printer -o $@ $^
#unit_tests.o: unit_tests.cc

#data_tests.cc: data_tests.hh
#	cxxtestgen --error-printer -o $@ $^

#unit_tests: unit_tests.o libdds.a
unit_tests.cc:
	cxxtestgen --root --error-printer -o $@ $<

unit_tests: $(UNIT_TESTS:.hh=.o) unit_tests.o libdds.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIB)
	./unit_tests

data_tests.cc: data_tests.hh
	cxxtestgen --error-printer -o $@ $<

data_tests: data_tests.o libdds.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIB)

timing_tests.cc: timing_tests.hh
	cxxtestgen --error-printer -o $@ $<

timing_tests: timing_tests.o libdds.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIB) -lboost_timer -lboost_system



%_tests.cc : %_tests.hh
	cxxtestgen --part --error-printer -o $@ $<

test: unit_tests

data_test: data_tests
	./data_tests

doc:
	doxygen dds.cfg

clean:
	-rm -f $(CXX_OBJ)
	-rm -f libdds.a dssim dds.so dds_module.o
	-rm -f $(UNIT_TESTS:.hh=.cc) $(UNIT_TESTS:.hh=.o)
	-rm -f $(TEST_SOURCES) $(TEST_SOURCES:.cc=.o) $(TEST_SOURCES:.cc=)

depend: $(CXX_SOURCES) $(TEST_SOURCES) $(UNIT_TESTS:.hh=.cc)
	$(CXX) $(CXXFLAGS) -MM $^ > .depend

include .depend
