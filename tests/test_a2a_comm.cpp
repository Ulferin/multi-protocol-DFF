/*
 * FastFlow concurrent network:
 *
 *                        | -> Rnode2 -> |    
 *                        |              |
 *           |-> Lnode1 ->| -> Rnode2 -> |
 *  Source ->|            |              | --> Forwarder --> Sink
 *           |-> Lnode2 ->| -> Rnode2 -> |
 *                        |              |
 *                        | -> Rnode2 -> |
 * 
 *
 *  distributed version:
 *
 *
 *     G0                        G1
 *   --------         -----------------------
 *  |        |       |           |-> Rnode1  |
 *  | Source | --->  | Lnode1 -->|           | -->|    -----------      ------
 *  |        |  |    |           |-> Rnode2  |    |   |           |    |      |
 *   --------   |     -----------------------     |-->| Forwarder |--> | Sink |
 *              |              |  ^               |   |           |    |      |
 *              |              |  |               |    -----------      ------
 *              |              v  |               |         G3            G4
 *              |     -----------------------     | 
 *               --> |           |-> Rnode3  | -->|  
 *                   | Lnode2 -->|           |
 *                   |           |-> Rnode4  |
 *                    -----------------------
 *                              G2
 *
 * 
 * 
 *                               --------
 *                         ---> |   G1   | ---> |
 *   --------             |      --------       |             --------            --------
 *  |   G0   | --Margo--> |        |  ^         | --Margo--> |   G3   | --TCP--> |   G4   |
 *   --------             |        v  |         |             --------            --------
 *                        |      --------       |            
 *			               ---> |   G2   | ---> |
 *						         --------
 *
 *
 * Where: 
 *	(G0 -> G1) |
 *	(G0 -> G2) |
 *	(G1 -> G2) |
 *	(G2 -> G1) |
 *	(G1 -> G3) |
 *	(G2 -> G3) |
 *			   |-------> Margo communicator
 *
 *	(G3 -> G4) |
 *			   |-------> TCP communicator
 *
 * 
 * 
 *
 *  ** MANUAL ** implementation of the distributed groups!
 *
 *  WARNING: This test is intended for FastFlow developers!!
 *
 */

#include <iostream>
#include <ff/dff.hpp>
#include <ff/distributed/ff_dadapters.hpp>
#include <ff/distributed/ff_dreceiver.hpp>
#include <ff/distributed/ff_dsender.hpp>
#include <mutex>
#include <map>

#include <ff_dAreceiverComp.hpp>
#include <ff_dAsenderComp.hpp>
#include <ff_dCommunicator.hpp>

using namespace ff;
std::mutex mtx;



struct myTask_t {
	std::string str;
	struct S_t {
		long  t;
		float f;
	} S;

	template<class Archive>
	void serialize(Archive & archive) {
		archive(str, S.t, S.f);
	}

};



struct Source : ff_monode_t<myTask_t>{
    myTask_t* svc(myTask_t*){
        int n = 10000;
        for(int i = 0; i < n; i++) {
            // std::this_thread::sleep_for(std::chrono::milliseconds(10));
			myTask_t* task = new myTask_t;
			task->str="Hello";
			task->S.t = i;
			task->S.f = i*1.0;
			ff_send_out(task);
		}
        printf("Generated %d tasks\n", n);
		return EOS;
    }
};

struct Lnode : ff_monode_t<myTask_t>{
    int numWorker, generatorID, count = 0;
    Lnode(int numWorker, int generatorID) : numWorker(numWorker), generatorID(generatorID) {}

    int svc_init() {
        std::cout << "Starting LNode " << generatorID << "\n";
        return 0;
    }

    myTask_t* svc(myTask_t* in){
        printf("LNode %d (%ld): starting generating tasks!\n", generatorID, get_my_id());
        printf("LNode %d (%ld): Received \"%s - [%ld, %f]\".\n", generatorID, get_my_id(), in->str.c_str(), in->S.t, in->S.f);
        // std::this_thread::sleep_for(std::chrono::milliseconds(10));
        for(int i = 0; i < numWorker; i++) {
            count++;
			myTask_t* out = new myTask_t;
			out->str = std::string(std::string("Task" + std::to_string(i) + " generated from " + std::to_string(generatorID) + " for " + std::to_string(i)));
			out->S.t = in->S.t * 2;
			out->S.f = in->S.f;
			
			ff_send_out_to(out, i);
        }
		delete in;
        return GO_ON;
    }

    void svc_end() {
        printf("Terminating LNode. Received EOS. Generated %d tasks.\n", count);
    }
};


struct Rnode : ff_minode_t<myTask_t>{
    int ID;
    Rnode(int id): ID(id) {}

    int svc_init() {
        std::cout << "Starting RNode " << ID << "\n";
        return 0;
    }

    myTask_t* svc(myTask_t* in){
        printf("RNode %d: Received \"%s - [%ld, %f]\".\n", ID, in->str.c_str(), in->S.t, in->S.f);
        in->str =std::string(std::string(in->str + " received by Rnode " + std::to_string(ID) + " from channel " +  std::to_string(get_channel_id())));
        in->S.f *= 2.;
        return in;
    }
};

struct Forwarder : ff_node_t<myTask_t> {

    myTask_t* svc(myTask_t* in) {
		std::cout << in->str << std::endl;
        return in;
    }

    void svc_end() {
        printf("Terminating forwarder node.\n");
    }

};

struct Sink : ff_node_t<myTask_t>{
    int count = 0;
    float sum = 0;

    myTask_t* svc(myTask_t* in){
        const std::lock_guard<std::mutex> lock(mtx);
		std::cout << in->str << std::endl;
		sum += in->S.t;
        sum += in->S.f;
        delete in;
        count++;
        return this->GO_ON;
    }

    void svc_end() {
        printf("I've received: %d tasks and the sum is: %f\n", count, sum);
    }
	
};


struct ForwarderNode : ff_node{ 
        ForwarderNode(std::function<void(void*, dataBuffer&)> f){
            this->serializeF = f;
        }
        ForwarderNode(std::function<void*(dataBuffer&)> f){
            this->deserializeF = f;
        }
        void* svc(void* input){return input;}
};


int main(int argc, char*argv[]){

    if (argc != 2){
        std::cerr << "Execute with the index of process!" << std::endl;
        return 1;
    }
    margo_set_environment(NULL);
    // margo_set_global_log_level(MARGO_LOG_TRACE);
    ABT_init(0, NULL);

#define LOCAL_TEST

#ifdef REMOTE_TEST
    /* --- TCP HANDSHAKE ENDPOINTS --- */
    ff_endpoint g1("38.242.220.197", 65005);
    g1.groupName = "G1";

    ff_endpoint g2("79.27.5.20", 56002);
    g2.groupName = "G2";

    ff_endpoint g3("38.242.220.197", 65004);
    g3.groupName = "G3";

    ff_endpoint g4("79.27.5.20", 56003);
    g3.groupName = "G4";
    /* --- TCP HANDSHAKE ENDPOINTS --- */


    /* --- RPC ENDPOINTS --- */
    ff_endpoint_rpc G0toG1_rpc("38.242.220.197", 65000, "ofi+sockets");
    ff_endpoint_rpc G2toG1_rpc("38.242.220.197", 65001, "ofi+sockets");

    ff_endpoint_rpc G0toG2_rpc("127.0.0.1", 56000, "ofi+sockets");
    ff_endpoint_rpc G1toG2_rpc("79.27.5.20", 56001, "ofi+sockets");
    ff_endpoint_rpc local_G1toG2_rpc("192.168.1.17", 56001, "ofi+sockets");

    ff_endpoint_rpc G1toG3_rpc("38.242.220.197", 65002, "ofi+sockets");
    ff_endpoint_rpc G2toG3_rpc("38.242.220.197", 65003, "ofi+sockets");
    /* --- RPC ENDPOINTS --- */
#endif


#ifdef LOCAL_TEST
        /* --- TCP HANDSHAKE ENDPOINTS --- */
    ff_endpoint g1("127.0.0.1", 65005);
    g1.groupName = "G1";

    ff_endpoint g2("127.0.0.1", 56002);
    g2.groupName = "G2";

    ff_endpoint g3("127.0.0.1", 65004);
    g3.groupName = "G3";

    ff_endpoint g4("127.0.0.1", 65006);
    g3.groupName = "G4";
    /* --- TCP HANDSHAKE ENDPOINTS --- */


    /* --- RPC ENDPOINTS --- */
    ff_endpoint_rpc G0toG1_rpc("127.0.0.1", 65000, "ofi+sockets");
    ff_endpoint_rpc G2toG1_rpc("127.0.0.1", 65001, "ofi+sockets");

    ff_endpoint_rpc G0toG2_rpc("127.0.0.1", 56000, "ofi+sockets");
    ff_endpoint_rpc local_G1toG2_rpc("127.0.0.1", 56001, "ofi+sockets");
    ff_endpoint_rpc G1toG2_rpc("127.0.0.1", 56001, "ofi+sockets");

    ff_endpoint_rpc G1toG3_rpc("127.0.0.1", 65002, "ofi+sockets");
    ff_endpoint_rpc G2toG3_rpc("127.0.0.1", 65003, "ofi+sockets");
    /* --- RPC ENDPOINTS --- */
#endif

    ff_farm gFarm;
    ff_a2a a2a;

    if (atoi(argv[1]) == 0) {
        gFarm.add_workers({new WrapperOUT(new Source(), 1, true)});
        // gFarm.add_collector(new ff_dAsender(new TransportTCPS({g1, g2}, "G0", {})));                             //TCP
        // gFarm.add_collector(new ff_dsender({g1, g2}, "G0"));                                                 //ORIGINAL

        gFarm.add_collector(new ff_dAsender(                                                                 //RPC
                new TransportRPCS({g1,g2}, {&G0toG1_rpc, &G0toG2_rpc}, "G0", {}, false, true), -1, 1));       //RPC

        gFarm.run_and_wait_end();
        ABT_finalize();
        return 0;
    } else if (atoi(argv[1]) == 1){
        // gFarm.add_emitter(new ff_dAreceiver(new TransportTCP(g1, true, {0,1}, {{0,0}}, {"G2"}), 2, -1, 1));      //TCP
        // gFarm.add_collector(new ff_dAsender(new TransportTCPS({g2, g3}, "G1", {"G2"})));                         //TCP

        // gFarm.add_emitter(new ff_dreceiverH(g1, 2, {{0, 0}}, {0,1}, {"G2"}));                                //ORIGINAL
        // gFarm.add_collector(new ff_dsenderH({g2,g3}, "G1", {"G2"}));                                         //ORIGINAL


        gFarm.add_emitter(new ff_dAreceiver(                                                                 //RPC
            new TransportRPC(g1, true, true, {&G0toG1_rpc, &G2toG1_rpc}, {0,1}, {{0, 0}}, {"G2"}),            //RPC
            2, -1, 1));                                                                                      //RPC
        gFarm.add_collector(new ff_dAsender(                                                                 //RPC
                new TransportRPCS({g2,g3}, {&G1toG2_rpc, &G1toG3_rpc}, "G1", {"G2"}, true, true), -1, 1));    //RPC

        gFarm.cleanup_emitter();
		gFarm.cleanup_collector();

		auto s = new Lnode(4,0);
		
        auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF)), new EmitterAdapter(s, 4, 0, {{0,0}, {1,1}}, true), true, true);

        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({{0,0}, {1,1}})});
        auto rnode0 = new Rnode(0);
		auto rnode1 = new Rnode(1);
        a2a.add_secondset<ff_node>({
									new ff_comb(new CollectorAdapter(rnode0, {0}, true),
												new WrapperOUT(new ForwarderNode(rnode0->serializeF), 0, 1, true)),
									new ff_comb(new CollectorAdapter(rnode1, {0}, true),
												new WrapperOUT(new ForwarderNode(rnode1->serializeF), 1, 1, true)),
									new SquareBoxRight});  // this box should be the last one!

    } else if (atoi(argv[1]) == 2) {
        // gFarm.add_emitter(new ff_dAreceiver(new TransportTCP(g2, true, {2,3}, {{1,0}}, {"G1"}), 2, -1, 1));      //TCP
        // gFarm.add_collector(new ff_dAsender(new TransportTCPS({g1, g3}, "G2", {"G1"})));                         //TCP
        
        // gFarm.add_emitter(new ff_dreceiverH(g2, 2, {{1, 0}}, {2,3}, {"G1"}));                                //ORIGINAL
        // gFarm.add_collector(new ff_dsenderH({g1, g3}, "G2", {"G1"}));                                        //ORIGINAL

        gFarm.add_emitter(new ff_dAreceiver(                                                                 //RPC
            new TransportRPC(g2, true, true, {&G0toG2_rpc, &local_G1toG2_rpc}, {2,3}, {{1, 0}}, {"G1"}),      //RPC
            2, -1, 1));                                                                                      //RPC
        
        gFarm.add_collector(new ff_dAsender(                                                                 //RPC
                new TransportRPCS({g1, g3}, {&G2toG1_rpc, &G2toG3_rpc}, "G2", {"G1"}, true, true), -1, 1));   //RPC


		gFarm.cleanup_emitter();
		gFarm.cleanup_collector();

		auto s = new Lnode(4,1);                                                                           
		auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF)), new EmitterAdapter(s, 4, 1, {{2,0}, {3,1}}, true), true, true);

        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({std::make_pair(2,0), std::make_pair(3,1)})}, 0, true);

        auto rnode2 = new Rnode(2);
		auto rnode3 = new Rnode(3);
		a2a.add_secondset<ff_node>({
									new ff_comb(new CollectorAdapter(rnode2, {1}, true),
												new WrapperOUT(new ForwarderNode(rnode2->serializeF), 2, 1, true), true, true),
									new ff_comb(new CollectorAdapter(rnode3, {1}, true),
												new WrapperOUT(new ForwarderNode(rnode3->serializeF), 3, 1, true), true, true),
									new SquareBoxRight   // this box should be the last one!
			                        }, true);		
        
    } else if (atoi(argv[1]) == 3) {
        // gFarm.add_emitter(new ff_dAreceiver(new TransportTCP(g3, false, {}, {{0,0}}, {}), 2, -1, 1));    //TCP
        // gFarm.add_collector(new ff_dAsender(new TransportTCPS({g4}, "G3")));                             //TCP

        // gFarm.add_emitter(new ff_dreceiver(g3, 2));                                                  //ORIGINAL
        gFarm.add_collector(new ff_dsender({g4}, "G3"));                                             //ORIGINAL

        gFarm.add_emitter(new ff_dAreceiver(                                                         //RPC
            new TransportRPC(g3, false, true, {&G1toG3_rpc, &G2toG3_rpc}, {},                         //RPC
                    {std::make_pair(0, 0)}, {}),                                                     //RPC
            2, -1, 1));                                                                              //RPC


		gFarm.add_workers({new WrapperINOUT(new Forwarder(), 0, 1, true)});
		
		gFarm.run_and_wait_end();
        ABT_finalize();
		return 0;
    } else {
        gFarm.add_emitter(new ff_dreceiver(g4, 1));                                                  //ORIGINAL
        // gFarm.add_emitter(new ff_dAreceiver(new TransportTCP(g4, false),1, -1, 1));                      
		gFarm.add_workers({new WrapperIN(new Sink(), 1, true)});
		
		gFarm.run_and_wait_end();
        ABT_finalize();
		return 0;
    }

    gFarm.add_workers({&a2a});
    gFarm.run_and_wait_end();
    ABT_finalize();
	return 0;
}
