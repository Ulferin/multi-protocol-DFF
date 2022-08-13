CXX = g++-10
CXXFLAGS = -Wall -g -O0 -std=c++20 -fno-inline-functions

INSTALL_PATH = /home/federico/install/lib
INCLUDE_PATH = ${HOME}/install/include

FF_ROOT = ${HOME}/includes/fastflow/
CEREAL_ROOT = ${HOME}/includes/cereal-1.3.2/include/
DEFINES = -DDFF_EXCLUDE_MPI -DMARGO_FF

LDFLAGS = -L$(INSTALL_PATH) -Wl,-rpath,$(INSTALL_PATH) -I${FF_ROOT} -I$(CEREAL_ROOT) -I$(INCLUDE_PATH)
LDLIBS = -pthread -lrt -lmargo -labt -lmercury -ljson-c
DEBUG = 

TARGETS = ff_g1.out ff_g2.out ff_g3.out margo_ff_test.out
DEPS = my-rpc.h ff_comm.hpp ff_margo_utils.hpp
FF_DEPS = ff_dreceiver_rpc.hpp ff_dsender_rpc.hpp ff_drpc_types.h ff_margo_utils.hpp

.PHONY = all clean cleanall

all: $(TARGETS)

ff_g1.out: ff_g1.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

ff_g2.out: ff_g2.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

ff_g3.out: ff_g3.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

margo_ff_test.out: test_a2a_h3.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

clean:
	rm -f ./*.o

cleanall: clean
	rm -f $(TARGETS)
	rm -f ./*.out
