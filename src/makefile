CXX = g++-10
CXXFLAGS = -Wall -g -O0 -std=c++20 -fno-inline-functions

# IMPORTANT: change these according to the installation path of Margo-related libraries
INSTALL_PATH = /usr/local/lib # /home/federico/install/lib
INCLUDE_PATH = /usr/local/include # ${HOME}/install/include

FF_ROOT = ${HOME}/includes/fastflow/
CEREAL_ROOT = ${HOME}/includes/cereal-1.3.2/include/
DEFINES = -DDFF_EXCLUDE_MPI

LDFLAGS = -L$(INSTALL_PATH) -Wl,-rpath,$(INSTALL_PATH) -I${FF_ROOT} -I$(CEREAL_ROOT) -I$(INCLUDE_PATH)
LDLIBS = -pthread -lrt -lmargo -labt -lmercury -ljson-c
DEBUG = 

TARGETS = comm_test.out
FF_DEPS = ff_dAreceiver.hpp ff_dAsender.hpp ff_dCommunicator.hpp ff_drpc_types.h ff_margo_utils.hpp ff_dCommI.hpp

.PHONY = all clean cleanall

all: $(TARGETS)

comm_test.out: test_a2a_comm.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

margo_ff_mpi.out: test_ff_mpi.cpp $(FF_DEPS)
	mpicxx $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

clean:
	rm -f ./*.o

cleanall: clean
	rm -f $(TARGETS)
	rm -f ./*.out
