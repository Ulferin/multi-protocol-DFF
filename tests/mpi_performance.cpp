/*
 * Application topology for performance test, reported in thesis Chapter 4
 *
 *                             |-> RNode1 ->|  
 *                             |            | 
 *           |-> LNode1 ->|    |-> RNode2 ->|
 *  Src ->   |            | -> |            |-> Snk
 *           |-> LNode2 ->|    |-> RNode3 ->|
 *                             |            |
 *                             |-> RNode4 ->|
 *          
 *
 * 
 *  G0: Src
 *  G1: LNode1, Rnode1, Rnode2
 *  G2: LNode2, RNode3, RNode4
 *  G3: Snk
 *
 * 
 * Builds 4 different distributed groups connecting them with MPI transport.
 * Three version are provided:
 *   - MPISP: original single protocol implementation of FastFlow MPI nodes,
 *            used as a baseline to compare performances;
 *   - MPIMP: extended multi-protocol implementation of the MPI component
 *   - MPIMIX: mixed MPI and TCP communication in the same MPI job.
 *
 * 
 * Execution example for TCPSP version:
 * mpirun -n 4 --hostfile myhosts --map-by node sh -c "./MPISP_performance.out 0 <ntasks> <msgsize> <lmswait> <rmswait>"
 * 
 * NOTE: to properly run this test the test_mpi.sh can be taken as a guide.
 *       Four different runs must be performed, using group IDs from 3 to 0.
 * 
 * Author:
 *      Federico Finocchio
 * 
 * Based on the original work from:
 *      Massimo Torquati
 *      Nicolo' Tonci
 * 
 */

#include <iostream>
#include <mutex>
#include <map>
#include <chrono>

#include <ff/dff.hpp>
#include <ff/distributed/ff_dadapters.hpp>
#include <ff/distributed/ff_dsenderMPI.hpp>
#include <ff/distributed/ff_dreceiverMPI.hpp>

#include <ff_dTransportType.hpp>
#include <ff_dMPreceiver.hpp>
#include <ff_dMPsender.hpp>
#include <ff_dManager.hpp>
#include <mpi.h>

using namespace ff;
std::mutex mtx;

static inline float active_delay(int msecs) {
  // read current time
  float x = 1.25f;
  auto start = std::chrono::high_resolution_clock::now();
  auto end   = false;
  while(!end) {
    auto elapsed = std::chrono::high_resolution_clock::now() - start;
    auto msec = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    x *= sin(x) / atan(x) * tanh(x) * sqrt(x);
    if(msec>=msecs)
      end = true;
  }
  return x;
}


// this assert will not be removed by -DNDEBUG
#define myassert(c) {													\
		if (!(c)) {														\
			std::cerr << "ERROR: myassert at line " << __LINE__ << " failed\n"; \
			abort();													\
		}																\
	}

struct ExcType {
	ExcType():contiguous(false) {}
	ExcType(bool): contiguous(true) {}
	~ExcType() {
		if (!contiguous)
			delete [] C;
	}
	
	size_t clen = 0;
	char*  C    = nullptr;
	bool contiguous;
	
#if !defined(MANUAL_SERIALIZATION)
	template<class Archive>
	void serialize(Archive & archive) {
	  archive(clen);
	  if (!C) {
		  myassert(!contiguous);
		  C = new char[clen];
	  }
	  archive(cereal::binary_data(C, clen));
	}
#endif
};


static ExcType* allocateExcType(size_t size, bool setdata=false) {
	char* _p = (char*)calloc(size+sizeof(ExcType), 1);	// to make valgrind happy !
	ExcType* p = new (_p) ExcType(true);  // contiguous allocation
	
	p->clen    = size;
	p->C       = (char*)p+sizeof(ExcType);
	if (setdata) {
		p->C[0]       = 'c';
		if (size>10) 
			p->C[10]  = 'i';
		if (size>100)
			p->C[100] = 'a';
		if (size>500)
			p->C[500] = 'o';		
	}
	p->C[p->clen-1] = 'F';
	return p;
}


struct Src : ff_monode_t<ExcType>{
    int ntask;
    Src(int ntask) : ntask(ntask){}

    ExcType* svc(ExcType*){
        for(int i = 0; i < ntask; i++) {
            ff_send_out(new ExcType);
        }
        return this->EOS;
    }

    void svc_end(){
        const std::lock_guard<std::mutex> lock(mtx);
        ff::cout << "[Source" << this->get_my_id() << "] Generated Items: " << ntask << ff::endl;
    }
};

struct LNode : ff_monode_t<ExcType>{
    int numWorker, execTime, generatorID, processedItems = 0;
    long dataLength;
    bool setdata;

    LNode(int generatorID, int numWorker, int execTime, long dataLength, bool setdata) :
        generatorID(generatorID), numWorker(numWorker), execTime(execTime),
        dataLength(dataLength), setdata(setdata) {}

    ExcType* svc(ExcType* in){
        processedItems++;
        if (execTime) active_delay(this->execTime);
        for(int i = 0; i < numWorker; i++) {
            ff_send_out_to(allocateExcType(dataLength, setdata), i);
        }
        return this->GO_ON;
    }

    void svc_end(){
        const std::lock_guard<std::mutex> lock(mtx);
        ff::cout << "[LNode" << this->get_my_id() << "] Processed Items: " << processedItems << ff::endl;
    }
};


struct RNode : ff_minode_t<ExcType>{
    int id, execTime, processedItems = 0;

    RNode(int id, int execTime): id(id), execTime(execTime) {}

    ExcType* svc(ExcType* in){
        processedItems++;
        if (execTime) active_delay(this->execTime);
        //printf("Waiting: %d\n", processedItems);
        // std::cout << "SERIALIZABLE? " << isSerializable() << "\n";
        if (in->C[in->clen-1] != 'F') {
            ff::cout << "ERROR: " << in->C[in->clen-1] << " != 'F'\n";
            myassert(in->C[in->clen-1] == 'F');
        }
        return in;
    }

    void svc_end(){
        const std::lock_guard<std::mutex> lock(mtx);
        ff::cout << "[RNode" << this->get_my_id() << "] Processed Items: " << processedItems << ff::endl;
    }

    
};

struct Snk : ff_node_t<ExcType>{
    int processedItems = 0, expected;
	bool checkdata;

    Snk(int expected, bool checkdata): expected(expected),
        checkdata(checkdata) {}

    ExcType* svc(ExcType* in){
        // std::cout << "SERIALIZABLE? " << isSerializable() << "\n";
        ++processedItems;
        if (checkdata) {
            myassert(in->C[0]     == 'c');
            if (in->clen>10) 
                myassert(in->C[10]  == 'i');
            if (in->clen>100)
                myassert(in->C[100] == 'a');
            if (in->clen>500)
                myassert(in->C[500] == 'o');
        }
        if (in->C[in->clen-1] != 'F') {
            ff::cout << "ERROR: " << in->C[in->clen-1] << " != 'F'\n";
            myassert(in->C[in->clen-1] == 'F');
        }	  
        delete in;
        return this->GO_ON;
    }

    void svc_end(){
        const std::lock_guard<std::mutex> lock(mtx);
        ff::cout << "[Sink" << this->get_my_id() << "] Processed Items: " << processedItems << ff::endl;
        if(processedItems < expected) {
            ff::cout << "I didn't receive all the expected (" << expected << ") items." << ff::endl;
        }
    }
};


struct ForwarderNode : ff_node { 
        ForwarderNode(std::function<bool(void*, dataBuffer&)> f,
					  std::function<void(void*)> d) {			
            this->serializeF = f;
			this->freetaskF  = d;
        }
        ForwarderNode(std::function<void*(dataBuffer&,bool&)> f,
					  std::function<void*(char*,size_t)> a) {
			this->alloctaskF   = a;
            this->deserializeF = f;
        }
        void* svc(void* input){ return input;}
    };


int main(int argc, char*argv[]){

    if (argc < 6){
        std::cout << "Usage: " << argv[0] << " #id #items #byteXitem #execTimeSource #execTimeSink"  << std::endl;
        return 1;
    }

    bool check = false;
    int items = atoi(argv[2]);
    long bytexItem = atol(argv[3]);
    int execTimeSource = atoi(argv[4]);
    int execTimeSink = atoi(argv[5]);

    char* p=nullptr;
	if ((p=getenv("CHECK_DATA"))!=nullptr) check=true;
	printf("checkdata = %d\n", check);

    int provided;
    if (MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided) != MPI_SUCCESS)
        return -1;  
      
    // no thread support 
    if (provided < MPI_THREAD_MULTIPLE){
        error("No thread support by MPI\n");
        return -1;
    }

    int myrank;
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);


    ff_endpoint g1(1);
    g1.groupName = "G1";

    ff_endpoint g2(2);
    g2.groupName = "G2";

    ff_endpoint g3(3);
    g3.groupName = "G3";

    ff_endpoint g1_tcp("172.16.34.2", 49001);
    g1_tcp.groupName = "G1";

    ff_endpoint g2_tcp("172.16.34.3", 49002);
    g2_tcp.groupName = "G2";

    ff_endpoint g3_tcp("172.16.34.4", 49003);
    g3_tcp.groupName = "G3";

    ff_farm gFarm;
    ff_a2a a2a;
    std::map<std::pair<std::string, ChannelType>, std::vector<int>> rt;
    if (myrank == 0){
        rt[std::make_pair(g1.groupName, ChannelType::FWD)] = std::vector<int>({0});
        rt[std::make_pair(g2.groupName, ChannelType::FWD)] = std::vector<int>({1});

        #if defined(MPIMIX)
	    SenderManager* sendMaster = new SenderManager({{{g1_tcp.groupName, g2_tcp.groupName}, new SenderPluginTCP({{ChannelType::FWD, g1_tcp},{ChannelType::FWD, g2_tcp}}, "G0")}}, &rt);
        gFarm.add_collector(new ff_dMPsender(sendMaster));
	    #elif defined(MPIMP)
        SenderManager* sendMaster = new SenderManager({{{g1.groupName, g2.groupName}, new SenderPluginMPI({{ChannelType::FWD, g1},{ChannelType::FWD, g2}}, "G0")}}, &rt);
        gFarm.add_collector(new ff_dMPsender(sendMaster));
        #elif defined(MPISP)
        gFarm.add_collector(new ff_dsenderMPI({{ChannelType::FWD, g1}, {ChannelType::FWD, g2}}, &rt,"G0"));
        #endif

        gFarm.add_workers({new WrapperOUT(new Src(items), 0, 1, 0, true)});
        gFarm.cleanup_collector();
        gFarm.run_and_wait_end();
        if (MPI_Finalize() != MPI_SUCCESS) abort();
        return 0;
    } else if (myrank == 1){
        rt[std::make_pair(g2.groupName, ChannelType::INT)] = std::vector<int>({2,3});
        rt[std::make_pair(g3.groupName, ChannelType::FWD)] = std::vector<int>({0});

        #if defined(MPIMIX)
        ReceiverManager *recMaster = new ReceiverManager({new ReceiverPluginMPI(1),new ReceiverPluginTCP(g1_tcp, 1)}, {{0, 0}});
        SenderManager* sendMaster = new SenderManager({{{g3.groupName}, new SenderPluginTCP({{ChannelType::FWD, g3_tcp}}, "G1")}, {{g2.groupName}, new SenderPluginMPI({{ChannelType::INT, g2}}, "G1")}}, &rt);
        gFarm.add_emitter(new ff_dMPreceiverH(recMaster, 2));
        gFarm.add_collector(new ff_dMPsenderH(sendMaster));
        #elif defined(MPIMP)
        ReceiverManager *recMaster = new ReceiverManager({new ReceiverPluginMPI(2)}, {{0, 0}});
        SenderManager* sendMaster = new SenderManager({{{g2.groupName, g3.groupName}, new SenderPluginMPI({{ChannelType::INT, g2},{ChannelType::FWD, g3}}, "G1")}}, &rt);
        gFarm.add_emitter(new ff_dMPreceiverH(recMaster, 2));
        gFarm.add_collector(new ff_dMPsenderH(sendMaster));
        #elif defined(MPISP)
        gFarm.add_emitter(new ff_dreceiverHMPI(2, {{0, 0}}));
        gFarm.add_collector(new ff_dsenderHMPI({{ChannelType::INT, g2},{ChannelType::FWD, g3}}, &rt, "G1"));
        #endif


        gFarm.cleanup_emitter();
		gFarm.cleanup_collector();
		
        auto s = new LNode(0, 4, execTimeSource, bytexItem, check);
        auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF, s->alloctaskF)), new EmitterAdapter(s, 4, 0, {{0,0}, {1,1}}, true), true, true);


        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({{0,0}, {1,1}})});
        auto sink0 = new RNode(0, execTimeSink);
        auto sink1 = new RNode(1, execTimeSink);
        a2a.add_secondset<ff_node>({new ff_comb(new CollectorAdapter(sink0, {0}, true),
                                                new WrapperOUT(new ForwarderNode(sink0->serializeF, sink0->freetaskF), 0, 1, 0, true)),
                                    new ff_comb(new CollectorAdapter(sink1, {0}, true),
                                                new WrapperOUT(new ForwarderNode(sink1->serializeF, sink1->freetaskF), 1, 1, 0, true)),
                                    new SquareBoxRight});

    } else if (myrank == 2) {
        rt[std::make_pair(g1.groupName, ChannelType::INT)] = std::vector<int>({0,1});
        rt[std::make_pair(g3.groupName, ChannelType::FWD)] = std::vector<int>({0});

	    #if defined(MPIMIX)
	    ReceiverManager *recMaster = new ReceiverManager({new ReceiverPluginMPI(1),new ReceiverPluginTCP(g2_tcp, 1)}, {{1, 0}});
        SenderManager* sendMaster = new SenderManager({{{g3.groupName}, new SenderPluginTCP({{ChannelType::FWD, g3_tcp}}, "G2")}, {{g1.groupName}, new SenderPluginMPI({{ChannelType::INT, g1}}, "G2")}}, &rt);
        gFarm.add_emitter(new ff_dMPreceiverH(recMaster, 2));
        gFarm.add_collector(new ff_dMPsenderH(sendMaster));
        #elif defined(MPIMP)
        ReceiverManager *recMaster = new ReceiverManager({new ReceiverPluginMPI(2)}, {{1, 0}});
        SenderManager* sendMaster = new SenderManager({{{g1.groupName, g3.groupName}, new SenderPluginMPI({{ChannelType::INT, g1},{ChannelType::FWD, g3}}, "G2")}}, &rt);
        gFarm.add_emitter(new ff_dMPreceiverH(recMaster, 2));
        gFarm.add_collector(new ff_dMPsenderH(sendMaster));
        #elif defined(MPISP)
        gFarm.add_emitter(new ff_dreceiverHMPI(2, {{1, 0}}));
        gFarm.add_collector(new ff_dsenderHMPI({{ChannelType::INT, g1}, {ChannelType::FWD, g3}}, &rt, "G2"));
        #endif

		gFarm.cleanup_emitter();
		gFarm.cleanup_collector();

		auto s = new LNode(1, 4, execTimeSource, bytexItem, check);
		auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF, s->alloctaskF)), new EmitterAdapter(s, 4, 1, {{2,0}, {3,1}}, true), true, true);

        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({{2,0}, {3,1}})}, 0, true);

        auto sink2 = new RNode(2, execTimeSink);
        auto sink3 = new RNode(3, execTimeSink);

        a2a.add_secondset<ff_node>({
									new ff_comb(new CollectorAdapter(sink2, {1}, true),
												new WrapperOUT(new ForwarderNode(sink2->serializeF, sink2->freetaskF), 2, 1, 0, true), true, true),
                                    new ff_comb(new CollectorAdapter(sink3, {1}, true),
												new WrapperOUT(new ForwarderNode(sink3->serializeF, sink3->freetaskF), 3, 1, 0, true), true, true),
									new SquareBoxRight
			                        }, true);

		
        
    } else {
        std::string *test_type;
        test_type = new std::string("PERF");

        printf("-- Testing %s communication\n", test_type->c_str());
        int total_task = items * 2;
        int expected_completion = std::max(items * execTimeSource/2, items * execTimeSink);
        
        printf("Configuration || ntask: %d - LNode wait (ms per task): %d - RNode wait (ms per task): %d - byteXitem: %ld\n", items, execTimeSource, execTimeSink, bytexItem);
        printf("Total number of task to the Sink node: %d\n", total_task);
        printf("Expected completion time (in ms): %d\n", expected_completion);

        ffTime(START_TIME);

        std::string *tt;
        #if defined(MPIMIX)
        tt = new std::string("MPIMIX");  
        ReceiverManager *recMaster = new ReceiverManager({new ReceiverPluginTCP(g3_tcp, 2)});
	    gFarm.add_emitter(new ff_dMPreceiver(recMaster, 2));
        #elif defined(MPIMP)
	    tt = new std::string("MPIMP");
        ReceiverManager *recMaster = new ReceiverManager({new ReceiverPluginMPI(2)});
        gFarm.add_emitter(new ff_dMPreceiver(recMaster, 2));
        #elif defined(MPISP)
	    tt = new std::string("MPISP");
        gFarm.add_emitter(new ff_dreceiverMPI(2));
        #endif

        gFarm.cleanup_emitter();
        Snk *snk = new Snk(total_task, check);

        gFarm.add_workers({new WrapperIN(snk, 1, true)});

        gFarm.run_and_wait_end();

        ffTime(STOP_TIME);
        std::cout << "Time " << *tt << ": " << ffTime(GET_TIME) << "\n";
        std::cout << "Total tasks to the Sink: " << snk->processedItems << "\n\n";
        if (MPI_Finalize() != MPI_SUCCESS) abort();
        return 0;
    }
    gFarm.add_workers({&a2a});
    gFarm.run_and_wait_end();
    if (MPI_Finalize() != MPI_SUCCESS) abort();
    return 0;
}
