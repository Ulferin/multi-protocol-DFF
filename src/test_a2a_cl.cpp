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
#include <numeric>
#include <execution>

#include "ff_dAreceiver.hpp"
#include "ff_dAsender.hpp"
#include "ff_dCommunicator.hpp"

using namespace ff;



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
    int ntask;
    Source(int ntask) : ntask(ntask) {}

    myTask_t* svc(myTask_t*){
        for(int i = 0; i < ntask; i++) {
            // std::this_thread::sleep_for(std::chrono::milliseconds(1));
			myTask_t* task = new myTask_t;
			task->str="Hello";
			task->S.t = i;
			task->S.f = i*1.0;
			ff_send_out(task);
		}
        printf("Generated %d tasks\n", ntask);
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
			out->S.t = in->S.t;
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
    int ID, mswait;
    Rnode(int id, int mswait): ID(id), mswait(mswait) {}

    int svc_init() {
        std::cout << "Starting RNode " << ID << "\n";
        return 0;
    }

    myTask_t* svc(myTask_t* in){
        std::this_thread::sleep_for(std::chrono::milliseconds(mswait));
        printf("RNode %d: Received \"%s - [%ld, %f]\".\n", ID, in->str.c_str(), in->S.t, in->S.f);
        in->str =std::string(std::string(in->str + " received by Rnode " + std::to_string(ID) + " from channel " +  std::to_string(get_channel_id())));
        return in;
    }
};

struct Sink : ff_node_t<myTask_t>{
    int count = 0, count_prev = 0;
    long double sum = 0;
    double prev_t = 0, curr_t = 0;
    std::vector<double> tstamps;

    myTask_t* svc(myTask_t* in){
        count++;
        curr_t = ff::ffTime(STOP_TIME);
        if((curr_t - prev_t) >= 1000) {
            double tstamp = ((count - count_prev) / (curr_t - prev_t))*1000;
            tstamps.push_back(tstamp);
            std::cout << "msg/s: " << tstamp << "\n";
            count_prev = count;
            prev_t = curr_t;
        }
		sum += in->S.t;
        sum += in->S.f;
        delete in;
        return this->GO_ON;
    }

    void svc_end() {
        auto const v_size = static_cast<float>(tstamps.size());
        auto const avg = std::reduce(tstamps.begin(), tstamps.end()) / v_size;
        printf("I've received: %d tasks and the sum is: %Lf\n", count, sum);
        printf("Avg msg/s: %f\n", avg);
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

    if (argc < 2){
        std::cerr << "Execute with the index of process!" << std::endl;
        return 1;
    }

    int ntask = 10000;
    int mswait = 2;

    if(argc == 4) {
        ntask = atoi(argv[2]);
        mswait = atoi(argv[3]);
    }

    margo_set_environment(NULL);
    // margo_set_global_log_level(MARGO_LOG_TRACE);
    ABT_init(0, NULL);


    // /* --- TCP HANDSHAKE ENDPOINTS --- */
    // ff_endpoint g1("172.16.34.2", 65005);
    // g1.groupName = "G1";

    // ff_endpoint g2("172.16.34.3", 56002);
    // g2.groupName = "G2";

    // ff_endpoint g3("172.16.34.4", 65004);
    // g3.groupName = "G3";
    // /* --- TCP HANDSHAKE ENDPOINTS --- */


    // /* --- RPC ENDPOINTS --- */
    // ff_endpoint_rpc G0toG1_rpc("172.16.34.2", 65000, "ofi+sockets");
    // ff_endpoint_rpc G2toG1_rpc("172.16.34.2", 65001, "ofi+sockets");

    // ff_endpoint_rpc G0toG2_rpc("172.16.34.3", 56000, "ofi+sockets");
    // ff_endpoint_rpc G1toG2_rpc("172.16.34.3", 56001, "ofi+sockets");

    // ff_endpoint_rpc G1toG3_rpc("172.16.34.4", 65002, "ofi+sockets");
    // ff_endpoint_rpc G2toG3_rpc("172.16.34.4", 65003, "ofi+sockets");
    /* --- RPC ENDPOINTS --- */


    /* --- TCP HANDSHAKE ENDPOINTS --- */
    ff_endpoint g1("127.0.0.1", 65005);
    g1.groupName = "G1";

    ff_endpoint g2("127.0.0.1", 56002);
    g2.groupName = "G2";

    ff_endpoint g3("127.0.0.1", 65004);
    g3.groupName = "G3";
    /* --- TCP HANDSHAKE ENDPOINTS --- */


    /* --- RPC ENDPOINTS --- */
    ff_endpoint_rpc G0toG1_rpc("127.0.0.1", 65000, "ofi+sockets");
    ff_endpoint_rpc G2toG1_rpc("127.0.0.1", 65001, "ofi+sockets");

    ff_endpoint_rpc G0toG2_rpc("127.0.0.1", 56000, "ofi+sockets");
    ff_endpoint_rpc G1toG2_rpc("127.0.0.1", 56001, "ofi+sockets");

    ff_endpoint_rpc G1toG3_rpc("127.0.0.1", 65002, "ofi+sockets");
    ff_endpoint_rpc G2toG3_rpc("127.0.0.1", 65003, "ofi+sockets");
    /* --- RPC ENDPOINTS --- */

    ff_farm gFarm;
    ff_a2a a2a;

    if (atoi(argv[1]) == 0) {
        gFarm.add_workers({new WrapperOUT(new Source(ntask), 1, true)});
        #ifdef TCP_TEST
        gFarm.add_collector(new ff_dAsender(new ff_dCommTCPS({g1, g2}, "G0", {})));                             //TCP
        #endif

        // gFarm.add_collector(new ff_dsender({g1, g2}, "G0"));                                                 //ORIGINAL

        #ifdef RPC_TEST
        gFarm.add_collector(new ff_dAsender(                                                                 //RPC
                new ff_dCommRPCS({g1,g2}, {&G0toG1_rpc, &G0toG2_rpc}, "G0", {}, false, true), -1, 1));       //RPC
        #endif

        gFarm.run_and_wait_end();
        ABT_finalize();
        return 0;
    } else if (atoi(argv[1]) == 1){
        #ifdef TCP_TEST
        gFarm.add_emitter(new ff_dAreceiver(new ff_dCommTCP(g1, true, {0,1}, {{0,0}}, {"G2"}), 2, -1, 1));      //TCP
        gFarm.add_collector(new ff_dAsender(new ff_dCommTCPS({g2, g3}, "G1", {"G2"})));                         //TCP
        #endif

        // gFarm.add_emitter(new ff_dreceiverH(g1, 2, {{0, 0}}, {0,1}, {"G2"}));                                //ORIGINAL
        // gFarm.add_collector(new ff_dsenderH({g2,g3}, "G1", {"G2"}));                                         //ORIGINAL

        #ifdef RPC_TEST
        gFarm.add_emitter(new ff_dAreceiver(                                                                 //RPC
            new ff_dCommRPC(g1, true, true, {&G0toG1_rpc, &G2toG1_rpc}, {0,1}, {{0, 0}}, {"G2"}),            //RPC
            2, -1, 1));                                                                                      //RPC
        gFarm.add_collector(new ff_dAsender(                                                                 //RPC
                new ff_dCommRPCS({g2,g3}, {&G1toG2_rpc, &G1toG3_rpc}, "G1", {"G2"}, true, true), -1, 1));    //RPC
        #endif

        gFarm.cleanup_emitter();
		gFarm.cleanup_collector();

		auto s = new Lnode(4,0);
		
        auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF)), new EmitterAdapter(s, 4, 0, {{0,0}, {1,1}}, true), true, true);

        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({{0,0}, {1,1}})});
        auto rnode0 = new Rnode(0, mswait);
		auto rnode1 = new Rnode(1, mswait);
        a2a.add_secondset<ff_node>({
									new ff_comb(new CollectorAdapter(rnode0, {0}, true),
												new WrapperOUT(new ForwarderNode(rnode0->serializeF), 0, 1, true)),
									new ff_comb(new CollectorAdapter(rnode1, {0}, true),
												new WrapperOUT(new ForwarderNode(rnode1->serializeF), 1, 1, true)),
									new SquareBoxRight});  // this box should be the last one!

    } else if (atoi(argv[1]) == 2) {
        #ifdef TCP_TEST
        gFarm.add_emitter(new ff_dAreceiver(new ff_dCommTCP(g2, true, {2,3}, {{1,0}}, {"G1"}), 2, -1, 1));      //TCP
        gFarm.add_collector(new ff_dAsender(new ff_dCommTCPS({g1, g3}, "G2", {"G1"})));                         //TCP
        #endif
        // gFarm.add_emitter(new ff_dreceiverH(g2, 2, {{1, 0}}, {2,3}, {"G1"}));                                //ORIGINAL
        // gFarm.add_collector(new ff_dsenderH({g1, g3}, "G2", {"G1"}));                                        //ORIGINAL

        #ifdef RPC_TEST
        gFarm.add_emitter(new ff_dAreceiver(                                                                 //RPC
            new ff_dCommRPC(g2, true, true, {&G0toG2_rpc, &G1toG2_rpc}, {2,3}, {{1, 0}}, {"G1"}),      //RPC
            2, -1, 1));                                                                                      //RPC
        
        gFarm.add_collector(new ff_dAsender(                                                                 //RPC
                new ff_dCommRPCS({g1, g3}, {&G2toG1_rpc, &G2toG3_rpc}, "G2", {"G1"}, true, true), -1, 1));   //RPC
        #endif

		gFarm.cleanup_emitter();
		gFarm.cleanup_collector();

		auto s = new Lnode(4,1);                                                                           
		auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF)), new EmitterAdapter(s, 4, 1, {{2,0}, {3,1}}, true), true, true);

        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({std::make_pair(2,0), std::make_pair(3,1)})}, 0, true);

        auto rnode2 = new Rnode(2, mswait);
		auto rnode3 = new Rnode(3, mswait);
		a2a.add_secondset<ff_node>({
									new ff_comb(new CollectorAdapter(rnode2, {1}, true),
												new WrapperOUT(new ForwarderNode(rnode2->serializeF), 2, 1, true), true, true),
									new ff_comb(new CollectorAdapter(rnode3, {1}, true),
												new WrapperOUT(new ForwarderNode(rnode3->serializeF), 3, 1, true), true, true),
									new SquareBoxRight   // this box should be the last one!
			                        }, true);		
        
    } else {
        ffTime(START_TIME);
        // gFarm.add_emitter(new ff_dreceiver(g3, 2));                                                  //ORIGINAL
        
        #ifdef RPC_TEST
        gFarm.add_emitter(new ff_dAreceiver(                                                         //RPC
            new ff_dCommRPC(g3, false, true, {&G1toG3_rpc, &G2toG3_rpc}, {},                         //RPC
                    {std::make_pair(0, 0)}, {}),                                                     //RPC
            2, -1, 1));                                                                             //RPC
        #endif

        #ifdef TCP_TEST
        gFarm.add_emitter(new ff_dAreceiver(new ff_dCommTCP(g3, false),2, -1, 1));                      
		#endif
		
		gFarm.add_workers({new WrapperIN(new Sink(), 1, true)});
        gFarm.run_and_wait_end();
        
        ffTime(STOP_TIME);
        std::cout << "Time: " << ffTime(GET_TIME) << "\n";
        
        ABT_finalize();
		return 0;
    }

    gFarm.add_workers({&a2a});
    gFarm.run_and_wait_end();
    ABT_finalize();
	return 0;
}