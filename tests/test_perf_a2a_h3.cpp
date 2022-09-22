/*
 *           
 *           |-> Forwarder1 ->|    |-> Sink1 ->|  
 *  Source ->|                | -> |           | -> StringPrinter
 *           |-> Forwarder2 ->|    |-> Sink2 ->|
 *          
 *
 * 
 *  G0: Source
 *  G1: Forwarer1, Sink1
 *  G2: Forwarder2, Sink2
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
#include <ff_dAreceiverComp.hpp>
#include <ff_dAsenderComp.hpp>
#include <ff_dManager.hpp>

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
        for(int i = 0; i < numWorker; i++) {
            if (execTime) active_delay(this->execTime);
            printf("Allocating %ld\n", dataLength);
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

    ff_endpoint g1("127.0.0.1", 49001);
    g1.groupName = "G1";

    ff_endpoint g1_2("127.0.0.1", 49004);
    g1_2.groupName = "G1";

    ff_endpoint g2("127.0.0.1", 49002);
    g2.groupName = "G2";

    ff_endpoint g2_2("127.0.0.1", 49005);
    g2_2.groupName = "G2";

    ff_endpoint g3("127.0.0.1", 49003);
    g3.groupName = "G3";

    ff_farm gFarm;
    ff_a2a a2a;
    std::map<std::pair<std::string, ChannelType>, std::vector<int>> rt;
    if (atoi(argv[1]) == 0){
        rt[std::make_pair(g1.groupName, ChannelType::FWD)] = std::vector<int>({0});
        rt[std::make_pair(g2.groupName, ChannelType::FWD)] = std::vector<int>({1});

        SenderManager* sendMaster = new SenderManager({{{g1.groupName, g2.groupName}, new TransportTCPS({{ChannelType::FWD, g1},{ChannelType::FWD, g2}}, "G0")}}, &rt);

        gFarm.add_collector(new ff_dAsender(sendMaster));
        gFarm.add_workers({new WrapperOUT(new Src(items), 0, 1, 0, true)});

        gFarm.run_and_wait_end();
        return 0;
    } else if (atoi(argv[1]) == 1){
        rt[std::make_pair(g2.groupName, ChannelType::INT)] = std::vector<int>({1});
        rt[std::make_pair(g3.groupName, ChannelType::FWD)] = std::vector<int>({0});

        // ReceiverManager *recMaster = new ReceiverManager({new TransportTCP(g1, 1),new TransportTCP(g1_2, 1)}, {{0, 0}});
        // SenderManager* sendMaster = new SenderManager({{{g3.groupName}, new TransportTCPS({{ChannelType::FWD, g3}}, "G1")}, {{g2_2.groupName}, new TransportTCPS({{ChannelType::INT, g2_2}}, "G1")}}, &rt);

        ReceiverManager *recMaster = new ReceiverManager({new TransportTCP(g1, 2)}, {{0, 0}});
        SenderManager* sendMaster = new SenderManager({{{g2.groupName, g3.groupName}, new TransportTCPS({{ChannelType::INT, g2},{ChannelType::FWD, g3}}, "G1")}}, &rt);

        gFarm.add_emitter(new ff_dAreceiverH(recMaster, 2));
        gFarm.add_collector(new ff_dAsenderH(sendMaster));
        gFarm.cleanup_emitter();
		gFarm.cleanup_collector();
		
        auto s = new LNode(0, 2, execTimeSource, bytexItem, check);
        auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF, s->alloctaskF)), new EmitterAdapter(s, 2, 0, {{0,0}}, true), true, true);

        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({std::make_pair(0,0)})});
        auto sink = new RNode(0, execTimeSink);
        a2a.add_secondset<ff_node>({new ff_comb(new CollectorAdapter(sink, {0}, true), new WrapperOUT(new ForwarderNode(sink->serializeF, sink->freetaskF), 0, 1, 0, true)), new SquareBoxRight});

    } else if (atoi(argv[1]) == 2) {
        rt[std::make_pair(g1.groupName, ChannelType::INT)] = std::vector<int>({0});
        rt[std::make_pair(g3.groupName, ChannelType::FWD)] = std::vector<int>({0});

        // ReceiverManager *recMaster = new ReceiverManager({new TransportTCP(g2, 1),new TransportTCP(g2_2, 1)}, {{1, 0}});
        // SenderManager* sendMaster = new SenderManager({{{g3.groupName}, new TransportTCPS({{ChannelType::FWD, g3}}, "G2")}, {{g1.groupName}, new TransportTCPS({{ChannelType::INT, g1_2}}, "G2")}}, &rt);

        ReceiverManager *recMaster = new ReceiverManager({new TransportTCP(g2, 2)}, {{1, 0}});
        SenderManager* sendMaster = new SenderManager({{{g1.groupName, g3.groupName}, new TransportTCPS({{ChannelType::INT, g1},{ChannelType::FWD, g3}}, "G2")}}, &rt);

        gFarm.add_emitter(new ff_dAreceiverH(recMaster, 2));
        gFarm.add_collector(new ff_dAsenderH(sendMaster));
		gFarm.cleanup_emitter();
		gFarm.cleanup_collector();

		auto s = new LNode(1, 2, execTimeSource, bytexItem, check);
		auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF, s->alloctaskF)), new EmitterAdapter(s, 2, 1, {{1,0}}, true), true, true);

        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({std::make_pair(1,0)})}, 0, true);

        auto sink = new RNode(1, execTimeSink);
		a2a.add_secondset<ff_node>({
									new ff_comb(new CollectorAdapter(sink, {1}, true),
												new WrapperOUT(new ForwarderNode(sink->serializeF, sink->freetaskF), 1, 1, 0, true), true, true),
									new SquareBoxRight
			                        }, true);

		
        
    } else {
        std::string *test_type;
        test_type = new std::string("PERF");

        printf("-- Testing %s communication\n", test_type->c_str());
        int total_task = items * 2;
        int expected_completion = std::max(items * execTimeSource, items * execTimeSink);
        
        printf("Configuration || ntask: %d - LNode wait (ms per task): %d - RNode wait (ms per task): %d\n", items, execTimeSource, execTimeSink);
        printf("Total number of task to the Sink node: %d\n", total_task);
        printf("Expected completion time (in ms): %d\n", expected_completion);

        ffTime(START_TIME);


        ReceiverManager *recMaster = new ReceiverManager({new TransportTCP(g3, 2)});
        gFarm.add_emitter(new ff_dAreceiver(recMaster, 2));
        Snk *snk = new Snk(total_task, check);
        gFarm.add_workers({new WrapperIN(snk, 1, true)});

        gFarm.run_and_wait_end();

        ffTime(STOP_TIME);
        std::cout << "Time: " << ffTime(GET_TIME) << "\n";
        std::cout << "Total tasks to the Sink: " << snk->processedItems << "\n"; 
        return 0;
    }
    gFarm.add_workers({&a2a});
    gFarm.run_and_wait_end();
}
