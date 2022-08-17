CXX = g++-10
CXXFLAGS = -Wall -g -O0 -std=c++20 -fno-inline-functions

# IMPORTANT: change these according to the installation path of Margo-related libraries
INSTALL_PATH = /usr/local/lib # /home/federico/install/lib
INCLUDE_PATH = /usr/local/include # ${HOME}/install/include

FF_ROOT = ${HOME}/includes/fastflow/
CEREAL_ROOT = ${HOME}/includes/cereal-1.3.2/include/
DEFINES = -DDFF_EXCLUDE_MPI -DMARGO_FF

LDFLAGS = -L$(INSTALL_PATH) -Wl,-rpath,$(INSTALL_PATH) -I${FF_ROOT} -I$(CEREAL_ROOT) -I$(INCLUDE_PATH)
LDLIBS = -pthread -lrt -lmargo -labt -lmercury -ljson-c
DEBUG = 

TARGETS = ff_g1.out ff_g2.out ff_g3.out margo_ff_test.out
DEPS = my-rpc.h ff_comm.hpp ff_margo_utils.hpp
FF_DEPS = ff_dAreceiver.hpp ff_dAsender.hpp ff_dCommunicator.hpp ff_dreceiver_rpc.hpp ff_dsender_rpc.hpp ff_drpc_types.h ff_margo_utils.hpp

.PHONY = all clean cleanall

all: $(TARGETS)

../examples/ff_g1.out: ../examples/ff_g1.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

../examples/ff_g2.out: ../examples/ff_g2.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

../examples/ff_g3.out: ../examles/ff_g3.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

margo_ff_test.out: test_a2a_h3.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

comm_test.out: test_a2a_comm.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

margo_ff_mpi.out: test_ff_mpi.cpp $(FF_DEPS)
	mpicxx $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

clean:
	rm -f ./*.o

cleanall: clean
	rm -f $(TARGETS)
	rm -f ./*.out
