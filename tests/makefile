CXX = g++-10
CXXFLAGS = -O3 -std=c++20 -finline-functions -DNDEBUG

# IMPORTANT: change these according to the installation path of Margo-related libraries
INSTALL_PATH = ${HOME}/install/lib
INSTALL_PATH64 = ${HOME}/install/lib64
INCLUDE_PATH = ${HOME}/install/include

FF_ROOT = ${HOME}/includes/fastflow/
CEREAL_ROOT = ${HOME}/includes/cereal-1.3.2/include/
FF_MP_ROOT=../src/

DEFINES = -DDFF_EXCLUDE_MPI -DABT_MEM_MAX_NUM_STACKS=8 # the latter is needed for the UCX plugin in Margo

LDFLAGS = -L$(INSTALL_PATH) -L$(INSTALL_PATH64) -Wl,-rpath,$(INSTALL_PATH) -Wl,-rpath,$(INSTALL_PATH64) -I${FF_ROOT} -I$(CEREAL_ROOT) -I$(INCLUDE_PATH) -I$(FF_MP_ROOT)
LDLIBS_MARGO = -pthread -lrt -lmargo -labt -lmercury -ljson-c
LDLIBS = -pthread
DEBUG = 

TARGETS = comm_test.out tcp_cl_test.out rpc_cl_test.out pipe_cl_test.out mpi_cl_test.out
MARGO_FF_DEPS = $(FF_DEPS) $(FF_MP_ROOT)margo_components/ff_dCommunicator.hpp $(FF_MP_ROOT)margo_components/ff_drpc_types.h $(FF_MP_ROOT)margo_components/ff_margo_utils.hpp
FF_DEPS = $(FF_MP_ROOT)ff_dAreceiverComp.hpp $(FF_MP_ROOT)ff_dAsenderComp.hpp $(FF_MP_ROOT)ff_dTransportType.hpp $(FF_MP_ROOT)ff_dManager.hpp $(FF_MP_ROOT)ff_dManagerI.hpp $(FF_MP_ROOT)ff_dTransportTypeI.hpp

.PHONY = all clean cleanall

all: $(TARGETS)

# comm_test.out: test_a2a_comm.cpp $(FF_DEPS)
# 	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

rpc_cl_test.out: test_a2a_rpc.cpp $(MARGO_FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS_MARGO) $(DEFINES) -DRPC_TEST

perf.out: test_perf_a2a_h3.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

# pipe_cl_test.out: test_pipe.cpp $(FF_DEPS)
# 	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES) -DRPC_TEST

# margo_ff_mpi.out: test_ff_mpi.cpp $(FF_DEPS)
# 	mpicxx $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

mpi_cl_test.out: test_a2a_mpi.cpp $(FF_DEPS)
	mpicxx $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS)

tcp_cl_test.out: test_a2a_tcp.cpp $(FF_DEPS)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEFINES)

clean:
	rm -f ./*.o

cleanall: clean
	rm -f $(TARGETS)
	rm -f ./*.out
