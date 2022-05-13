CXX = g++-10
CXXFLAGS = -Wall -g -O0 -std=c++20 -fno-inline-functions

FF_ROOT = ${HOME}/includes/fastflow/
CEREAL_ROOT = ${HOME}/includes/cereal-1.3.2/include/
DEFINES = -DDFF_EXCLUDE_MPI -DMARGO_FF

LDFLAGS = -L/usr/local/lib -Wl,-rpath,/usr/local/lib/ -I${FF_ROOT} -I$(CEREAL_ROOT)
LDLIBS = -pthread -lrt -lmargo -labt -lmercury
DEBUG = 

TARGETS = ff_g1.out ff_g2.out ff_g3.out
DEPS = my-rpc.h ff_comm.hpp utils.hpp

.PHONY = all clean cleanall

all: $(TARGETS)

ff_g1.out: ff_g1.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

ff_g2.out: ff_g2.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

ff_g3.out: ff_g3.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

margo_ff_test.out: test_a2a_h3.cpp ff_dreceiver_rpc.hpp ff_dsender_rpc.hpp $(FF_ROOT)ff/distributed/ff_drpc_types.h $(FF_ROOT)ff/distributed/ff_dutils.hpp
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

clean:
	rm -f ./*.o

cleanall: clean
	rm -f $(TARGETS)
	rm -f ./*.out
