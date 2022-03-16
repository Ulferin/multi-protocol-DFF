CXX = g++
CXXFLAGS = -g -O3 -std=c++17 -finline-functions

FF_ROOT = ${HOME}/includes/fastflow/

LDFLAGS = -L/usr/local/lib -Wl,-rpath,/usr/local/lib/ -I${FF_ROOT}
LDLIBS = -pthread -lrt -lmargo -labt -lmercury
DEBUG = 

TARGETS = ff_g1.out ff_g2.out ff_g3.out

.PHONY = all clean cleanall

all: $(TARGETS)

ff_g1.out: ff_g1.cpp my-rpc.h
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

ff_g2.out: ff_g2.cpp my-rpc.h
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

ff_g3.out: ff_g3.cpp my-rpc.h
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS) $(DEBUG)

clean:
	rm -f ./*.o

cleanall:
	make clean
	rm -f $(TARGETS)
	rm -f ./*.out
