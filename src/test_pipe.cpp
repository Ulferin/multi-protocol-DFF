/*
 * Author: Federico Finocchio
 * Description: simple pipe structure to test network performance and debug
 * plugin functionalities
 * 
 * FastFlow concurrent network:
 *
 *  Source --> Forwarder --> Sink
 * 
 *
 *  distributed version:
 *
 *
 *     G0                  G1             G2
 *   --------         -----------       ------
 *  |        |       |           |     |      |
 *  | Source | --->  | Forwarder | --> | Sink |
 *  |        |       |           |     |      |
 *   --------         -----------       ------
 *
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
#include <algorithm>

#include "ff_dAreceiver.hpp"
#include "ff_dAsender.hpp"
#include "ff_dCommunicator.hpp"

using namespace ff;

#ifndef RPC_TEST
#define RPC_TEST
#endif

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
    int ntask, mswait;
    Source(int ntask, int mswait) : ntask(ntask), mswait(mswait) {}

    myTask_t* svc(myTask_t*){
        
        // std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        for(int i = 0; i < ntask; i++) {
            std::this_thread::sleep_for(std::chrono::milliseconds(mswait));
			myTask_t* task = new myTask_t;
			task->str="Hello";
			task->S.t = i;
			task->S.f = i*1.0;
			ff_send_out(task);
		}
        printf("Generated %d tasks\n", ntask);
        // std::this_thread::sleep_for(std::chrono::milliseconds(10000));
		return EOS;
    }
};


struct Forwarder : ff_node_t<myTask_t> {

    int expected_ntask, mswait;
    Forwarder(int expected_ntask, int mswait): expected_ntask(expected_ntask), mswait(mswait) {}

    myTask_t* svc(myTask_t* in) {
		std::this_thread::sleep_for(std::chrono::milliseconds(mswait));
        return in;
    }

    void svc_end() {
        printf("Terminating forwarder node.\n");
    }

};


struct Sink : ff_node_t<myTask_t>{
    int verbose = 0;
    Sink(int verbose=0) : verbose(verbose) {}

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
            if(verbose) std::cout << "msg/s: " << tstamp << "\n";
            count_prev = count;
            prev_t = curr_t;
        }
		sum += in->S.t;
        sum += in->S.f;
        delete in;
        return this->GO_ON;
    }

    void svc_end() {
        if(count - count_prev != 0) {
            curr_t = ff::ffTime(STOP_TIME);
            double tstamp = ((count - count_prev) / (curr_t - prev_t))*1000;
            tstamps.push_back(tstamp);
            if(verbose) std::cout << "msg/s: " << tstamp << "\n";
        }
        if(verbose) printf("I've received: %d tasks and the sum is: %Lf\n", count, sum);

        auto const v_size = static_cast<float>(tstamps.size());
        auto const avg = std::reduce(tstamps.begin(), tstamps.end()) / v_size;
        printf("msg/s || real avg: %f - approximate avg: %f\n", avg, (count / ff::ffTime(STOP_TIME))*1000);
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
    int Rmswait = 2;
    int Lmswait = 2;

    if(argc >= 5) {
        ntask = atoi(argv[2]);
        Lmswait = atoi(argv[3]);
        Rmswait = atoi(argv[4]);
    }
    const char* protocol = (argc >= 6) ? argv[5] : "ofi+sockets";
    int port = (argc >= 7) ? atoi(argv[6]) : 49000;
    int verbose = (argc >= 8) ? atoi(argv[7]) : 0;

    margo_set_environment(NULL);
    // margo_set_global_log_level(MARGO_LOG_TRACE);
    ABT_init(0, NULL);


    /* --- TCP HANDSHAKE ENDPOINTS --- */
    ff_endpoint g1("38.242.220.197", port);
    g1.groupName = "G1";

    ff_endpoint g2("38.242.220.197", port+1);
    g2.groupName = "G2";

    ff_endpoint g3("38.242.220.197", port+2);
    g3.groupName = "G3";
    /* --- TCP HANDSHAKE ENDPOINTS --- */


    /* --- RPC ENDPOINTS --- */
    ff_endpoint_rpc G0toG1_rpc("38.242.220.197", port+3, protocol);
    ff_endpoint_rpc G2toG1_rpc("38.242.220.197", port+4, protocol);

    ff_endpoint_rpc G0toG2_rpc("38.242.220.197", port+5, protocol);
    ff_endpoint_rpc G1toG2_rpc("38.242.220.197", port+6, protocol);

    ff_endpoint_rpc G1toG3_rpc("38.242.220.197", port+7, protocol);
    ff_endpoint_rpc G2toG3_rpc("38.242.220.197", port+8, protocol);
    /* --- RPC ENDPOINTS --- */

    ff_farm gFarm;
    ff_a2a a2a;

    if (atoi(argv[1]) == 0) {
        gFarm.add_workers({new WrapperOUT(new Source(ntask, Rmswait), 1, true)});
        #ifdef TCP_TEST
        gFarm.add_collector(new ff_dAsender(new ff_dCommTCPS({g1, g2}, "G0", {})));                             //TCP
        #endif

        // gFarm.add_collector(new ff_dsender({g1, g2}, "G0"));                                                 //ORIGINAL

        #ifdef RPC_TEST
        gFarm.add_collector(new ff_dAsender(                                                                 //RPC
                new ff_dCommRPCS({g1}, {&G0toG1_rpc}, "G0", {}, false, true), -1, 1));       //RPC
        #endif

        gFarm.run_and_wait_end();
        ABT_finalize();
        return 0;
    } else if (atoi(argv[1]) == 1){
        #ifdef RPC_TEST
        gFarm.add_collector(new ff_dAsender(                                                                 //RPC
                new ff_dCommRPCS({g3}, {&G1toG3_rpc}, "G1", {}, false, true), -1, 1));       //RPC
        
        gFarm.add_emitter(new ff_dAreceiver(                                                         //RPC
            new ff_dCommRPC(g1, false, true, {&G0toG1_rpc}, {},                         //RPC
                    {std::make_pair(0, 0)}, {}),                                                     //RPC
            1, -1, 1, ntask));                                                                              //RPC
        #endif

		gFarm.add_workers({new WrapperINOUT(new Forwarder(ntask, Lmswait), 0, 1, true)});
		
		gFarm.run_and_wait_end();
        ABT_finalize();
		return 0;

    } else {

        std::string *test_type;
        #ifdef RPC_TEST
        test_type = new std::string("RPC");
        #endif
        #ifdef TCP_TEST
        test_type = new std::string("TCP");
        #endif

        printf("-- Testing %s communication with \"%s\" --\n", test_type->c_str(), protocol);
        int total_task = ntask * 4;
        int expected_completion = std::max({0, Lmswait*ntask, Rmswait*ntask});
        
        printf("Configuration || ntask: %d - LNode wait (ms per task): %d - RNode wait (ms per task): %d\n", ntask, Lmswait, Rmswait);
        printf("Total number of task to the Sink node: %d\n", total_task);
        printf("Expected completion time (in ms): %d\n", expected_completion);

        ffTime(START_TIME);
        // gFarm.add_emitter(new ff_dreceiver(g3, 2));                                                  //ORIGINAL
        
        #ifdef RPC_TEST
        gFarm.add_emitter(new ff_dAreceiver(                                                         //RPC
            new ff_dCommRPC(g3, false, true, {&G1toG3_rpc, &G2toG3_rpc}, {},                         //RPC
                    {std::make_pair(0, 0)}, {}),                                                     //RPC
            1, -1, 1, ntask));                                                                             //RPC
        #endif

        #ifdef TCP_TEST
        gFarm.add_emitter(new ff_dAreceiver(new ff_dCommTCP(g3, false),2, -1, 1));                      
		#endif
		Sink *sink = new Sink(verbose);	
		gFarm.add_workers({new WrapperIN(sink, 1, true)});
        gFarm.run_and_wait_end();
        
        ffTime(STOP_TIME);
        std::cout << "Time: " << ffTime(GET_TIME) << "\n";
        std::cout << "Total tasks to the Sink: " << sink->count << "\n"; 
        ABT_finalize();
		return 0;
    }

    gFarm.add_workers({&a2a});
    gFarm.run_and_wait_end();
    ABT_finalize();
	return 0;
}
