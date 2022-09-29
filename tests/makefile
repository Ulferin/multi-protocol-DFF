CXX = g++-10
CXXFLAGS = -O3 -std=c++20 -finline-functions -DNDEBUG

# IMPORTANT: change these according to the installation path of Margo-related libraries
INSTALL_PATH = ${HOME}/install/lib
INSTALL_PATH64 = ${HOME}/install/lib64
INCLUDE_PATH = ${HOME}/install/include

FF_ROOT = ${HOME}/includes/fastflow/
CEREAL_ROOT = ${HOME}/includes/cereal-1.3.2/include/

# Options:
# 
#
# MARGO           		Must be defined to properly compile RPC tests, like
#						rpc_test.out and tcp_performance.out when using TRANS=TCPRPC
#                       in busy-waiting loops
#
# TRANS  				Used in the performance tests to specify the type of test
#						to run.
#						For MPI tests, accepted values are:
#							- MPISP: build the MPI test using original FastFlow MPI
#							- MPIMP: build the MPI test using extension MPI implementation
#						
#						For TCP tests, accepted values are:
#							- TCPSP: build the TCP test using original FastFlow TCP
#							- TCPMP: build the TCP test using extension TCP implementation
#							- TCPRPC: build the RCP test
#
#


ifdef FF_ROOT
    INCS            += -I$(FF_ROOT)
else
    INCS            += -I ~/fastflow
endif
ifdef CEREAL_ROOT
    INCS            += -I$(CEREAL_ROOT)
else
    INCS            += -I ~/cereal
endif

# MARGO should be defined for those tests relying on RPC functionalities
ifdef MARGO
	LDLIBS 			= -pthread -lrt -lmargo -labt -lmercury -ljson-c
else
	LDLIBS 			= -pthread
endif



FF_MP_ROOT=../src/

INCLUDES             = $(INCS)

DEFINES = -DDFF_EXCLUDE_MPI

LDFLAGS = -L$(INSTALL_PATH) -L$(INSTALL_PATH64) -Wl,-rpath,$(INSTALL_PATH) -Wl,-rpath,$(INSTALL_PATH64) ${INCS} -I$(INCLUDE_PATH) -I$(FF_MP_ROOT)

TARGETS = tcp_test.out rpc_test.out mpi_test.out tcp_performance.out mpi_performance.out mpi_inout.out tcp_inout.out
MARGO_FF_DEPS = $(FF_DEPS) $(FF_MP_ROOT)margo_components/ff_dCommunicator.hpp $(FF_MP_ROOT)margo_components/ff_drpc_types.h $(FF_MP_ROOT)margo_components/ff_margo_utils.hpp
FF_DEPS = $(FF_MP_ROOT)ff_dMPreceiver.hpp $(FF_MP_ROOT)ff_dMPsender.hpp $(FF_MP_ROOT)ff_dTransportType.hpp $(FF_MP_ROOT)ff_dManager.hpp $(FF_MP_ROOT)ff_dManagerI.hpp $(FF_MP_ROOT)ff_dTransportTypeI.hpp

.PHONY = all clean cleanall

all: $(TARGETS)

rpc_test.out: test_a2a_rpc.cpp $(MARGO_FF_DEPS)
	$(CXX) $(CXXFLAGS) -g $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

mpi_test.out: test_a2a_mpi.cpp $(FF_DEPS)
	mpicxx $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS)

tcp_test.out: test_a2a_tcp.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

# ----- PERFORMANCE TESTS: specify the type with TRANS=TCPSP/TCPMP/TCPRPC -----
tcp_performance.out: tcp_performance.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $(TRANS)_performance.out $(LDFLAGS) $(LDLIBS) $(DEFINES) -D$(TRANS)

# ----- PERFORMANCE TESTS: specify the type with TRANS=MPISP/MPIMP -----
mpi_performance.out: mpi_performance.cpp $(FF_DEPS)
	mpicxx $(CXXFLAGS) $< -o $(TRANS)_performance.out $(LDFLAGS) $(LDLIBS) -D$(TRANS)

mpi_inout.out: caseMPI_INOUT.cpp $(FF_DEPS)
	mpicxx $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) -D$(TRANS)

tcp_inout.out: caseTCP_INOUT.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)


clean:
	rm -f ./*.o ./*~

cleanall: clean
	rm -f $(TARGETS) ./*.out ./log.*
