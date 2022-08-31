CXX = g++-10
CXXFLAGS = -Wall -g -O3 -std=c++20 -fno-inline-functions

# IMPORTANT: change these according to the installation path of Margo-related libraries
INSTALL_PATH = /usr/local/lib # ${HOME}/install/lib
INSTALL_PATH64 = /home/federico/install/lib64
INCLUDE_PATH = /usr/local/include # ${HOME}/install/include

FF_ROOT = ${HOME}/includes/fastflow/
CEREAL_ROOT = ${HOME}/includes/cereal-1.3.2/include/
DEFINES = -DDFF_EXCLUDE_MPI

LDFLAGS = -L$(INSTALL_PATH) -L$(INSTALL_PATH64) -Wl,-rpath,$(INSTALL_PATH) -Wl,-rpath,$(INSTALL_PATH64) -I${FF_ROOT} -I$(CEREAL_ROOT) -I$(INCLUDE_PATH)
LDLIBS = -pthread -lrt -lmargo -labt -lmercury -ljson-c
DEBUG = 

TARGETS = comm_test.out tcp_cl_test.out rpc_cl_test.out
FF_DEPS = ff_dAreceiver.hpp ff_dAsender.hpp ff_dCommunicator.hpp ff_drpc_types.h ff_margo_utils.hpp ff_dCommI.hpp

.PHONY = all clean cleanall

all: $(TARGETS)

comm_test.out: test_a2a_comm.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

tcp_cl_test.out: test_a2a_cl.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES) -DTCP_TEST

rpc_cl_test.out: test_a2a_cl.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES) -DRPC_TEST

margo_ff_mpi.out: test_ff_mpi.cpp $(FF_DEPS)
	mpicxx $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

clean:
	rm -f ./*.o

cleanall: clean
	rm -f $(TARGETS)
	rm -f ./*.out
