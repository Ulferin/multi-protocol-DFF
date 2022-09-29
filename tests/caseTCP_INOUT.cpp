/*
  *                                 |-> Sink1 ->|  
 *                                 |           | 
 *           |-> Forwarder1 ->|    |-> Sink2 ->|
 *  Source ->|                | -> |           |-> StringPrinter
 *           |-> Forwarder2 ->|    |-> Sink3 ->|
 *                                 |           |
 *                                 |-> Sink4 ->|
 *          
 *
 * 
 *  G0: Source
 *  G1: Forwarer1, Sink1, Sink2
 *  G2: Forwarder2, Sink2, Sink3
 *  G3: StringPrinter
 *
 */

#include <iostream>
#include <mutex>
#include <map>
#include <chrono>

#include <ff/dff.hpp>
#include <ff/distributed/ff_dadapters.hpp>

#include <ff_dTransportType.hpp>
#include <ff_dMPreceiver.hpp>
#include <ff_dMPsender.hpp>
#include <ff_dManager.hpp>

using namespace ff;
std::mutex mtx;


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

struct Fwd : ff_node_t<ExcType>{
    ExcType* svc(ExcType* task){
        // std::cout << "Received" << std::endl;
        // delete task;
        // ff_send_out(task);

        return this->GO_ON;
    }
};

struct Snk : ff_node_t<ExcType>{
    int processedItems = 0, expected;
	bool checkdata;

    Snk(int expected, bool checkdata): expected(expected),
        checkdata(checkdata) {}

    ExcType* svc(ExcType* in){
        // std::cout << "Received" << std::endl;
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

    bool check = false;
    int items = atoi(argv[2]);
    long bytexItem = atol(argv[3]);
    int execTimeSource = atoi(argv[4]);
    int execTimeSink = atoi(argv[5]);

    char* p=nullptr;
	if ((p=getenv("CHECK_DATA"))!=nullptr) check=true;
	printf("checkdata = %d\n", check);

    ff_endpoint g1("127.0.0.1", 49000);
    g1.groupName = "G1";

    ff_endpoint g4("38.242.220.197", 65000);
    g4.groupName = "G4";

    ff_farm gFarm;
    ff_a2a a2a;
    std::map<std::pair<std::string, ChannelType>, std::vector<int>> rt;
    if (atoi(argv[1]) == 0){
        rt[std::make_pair(g1.groupName, ChannelType::FWD)] = std::vector<int>({0});

        SenderManager* sendMaster = new SenderManager({{{g1.groupName}, new SenderPluginTCP({{ChannelType::FWD, g1}}, "G0")}}, &rt);
        gFarm.add_collector(new ff_dMPsender(sendMaster));

        gFarm.add_workers({new WrapperOUT(new Src(items), 0, 1, 0, true)});
        gFarm.cleanup_collector();
        gFarm.run_and_wait_end();
        return 0;
    }

    else if (atoi(argv[1]) == 1){

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
        tt = new std::string("MPIMP");
        ReceiverManager *recMaster = new ReceiverManager({new ReceiverPluginTCP(g4, 1)});
        gFarm.add_emitter(new ff_dMPreceiver(recMaster, 1));

        gFarm.cleanup_emitter();
        Snk *snk = new Snk(total_task, check);

        gFarm.add_workers({new WrapperIN(snk, 1, true)});

        gFarm.run_and_wait_end();

        ffTime(STOP_TIME);
        std::cout << "Time " << *tt << ": " << ffTime(GET_TIME) << "\n";
        std::cout << "Total tasks to the Sink: " << snk->processedItems << "\n\n";
        return 0;
    }
}
