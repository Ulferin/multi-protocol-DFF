/*
 * FastFlow concurrent network:
 *
 *                        | -> Rnode2 -> |    
 *                        |              |
 *           |-> Lnode1 ->| -> Rnode2 -> |
 *  Source ->|            |              | --> Sink
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
 *  | Source | --->  | Lnode1 -->|           | -->|     ------
 *  |        |  |    |           |-> Rnode2  |    |    |      |
 *   --------   |     -----------------------     |--> | Sink |
 *              |              |  ^               |    |      |
 *              |              |  |               |     ------
 *              |              v  |               |       G4
 *              |     -----------------------     | 
 *               --> |           |-> Rnode3  | -->|  
 *                   | Lnode2 -->|           |
 *                   |           |-> Rnode4  |
 *                    -----------------------
 *                              G2
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
#include <algorithm>
#include <mpi.h>

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
    int ntask, mswait;
    Source(int ntask, int mswait) : ntask(ntask), mswait(mswait) {}

    myTask_t* svc(myTask_t*){
        for(int i = 0; i < ntask; i++) {
            std::this_thread::sleep_for(std::chrono::milliseconds(mswait));
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
    int numWorker, generatorID, count = 0, mswait=0;
    Lnode(int numWorker, int generatorID, int mswait=0) : numWorker(numWorker), generatorID(generatorID), mswait(mswait) {}

    int svc_init() {
        std::cout << "Starting LNode " << generatorID << "\n";
        return 0;
    }

    myTask_t* svc(myTask_t* in){
        // printf("LNode %d (%ld): starting generating tasks!\n", generatorID, get_my_id());
        // printf("LNode %d (%ld): Received \"%s - [%ld, %f]\".\n", generatorID, get_my_id(), in->str.c_str(), in->S.t, in->S.f);
        std::this_thread::sleep_for(std::chrono::milliseconds(mswait));
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
        // printf("RNode %d: Received \"%s - [%ld, %f]\".\n", ID, in->str.c_str(), in->S.t, in->S.f);
        // in->str =std::string(std::string(in->str + " received by Rnode " + std::to_string(ID) + " from channel " +  std::to_string(get_channel_id())));
        return in;
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

    int rank;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int ntask = 10;
    int Rmswait = 2;
    int Lmswait = 2;

    if(argc >= 5) {
        ntask = atoi(argv[2]);
        Lmswait = atoi(argv[3]);
        Rmswait = atoi(argv[4]);
    }
    const char* protocol = (argc >= 6) ? argv[5] : "ofi+sockets";
    int verbose = (argc >= 7) ? atoi(argv[6]) : 0;

    /* --- HANDSHAKE ENDPOINTS --- */
    ff_endpoint g1(1);
    g1.groupName = "G1";

    ff_endpoint g2(2);
    g2.groupName = "G2";

    ff_endpoint g3(3);
    g3.groupName = "G3";
    /* --- HANDSHAKE ENDPOINTS --- */

    ff_farm gFarm;
    ff_a2a a2a;

    printf("\n\nStarting!\n");

    if (rank == 0) {
        gFarm.add_workers({new WrapperOUT(new Source(ntask, Rmswait), 1, true)});
        // gFarm.add_collector(new ff_dAsender(new ff_dCommTCPS({g1, g2}, "G0", {})));                             //TCP
        gFarm.add_collector(new ff_dAsender(new ff_dCommMPIS({g1, g2}, "G0")));                             //MPI

        gFarm.run_and_wait_end();
        MPI_Finalize();
        return 0;
    } else if (rank == 1){
        // gFarm.add_emitter(new ff_dAreceiver(new ff_dCommTCP(g1, true, {0,1}, {{0,0}}, {"G2"}), 2, -1, 1));      //TCP
        // gFarm.add_collector(new ff_dAsender(new ff_dCommTCPS({g2, g3}, "G1", {"G2"})));                         //TCP

        gFarm.add_emitter(new ff_dAreceiver(new ff_dCommMPI(g1, true, {0,1}, {{0,0}}, {"G2"}, {2}), 2, -1, 1, (ntask/2)+ntask));      //MPI
        gFarm.add_collector(new ff_dAsender(new ff_dCommMPIS({g2, g3}, "G1", {"G2"}, {2})));                         //MPI


        gFarm.cleanup_emitter();
		gFarm.cleanup_collector();

		auto s = new Lnode(4,0,Lmswait);
		
        auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF)), new EmitterAdapter(s, 4, 0, {{0,0}, {1,1}}, true), true, true);

        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({{0,0}, {1,1}})});
        auto rnode0 = new Rnode(0, Rmswait);
		auto rnode1 = new Rnode(1, Rmswait);
        a2a.add_secondset<ff_node>({
									new ff_comb(new CollectorAdapter(rnode0, {0}, true),
												new WrapperOUT(new ForwarderNode(rnode0->serializeF), 0, 1, true)),
									new ff_comb(new CollectorAdapter(rnode1, {0}, true),
												new WrapperOUT(new ForwarderNode(rnode1->serializeF), 1, 1, true)),
									new SquareBoxRight});  // this box should be the last one!

    } else if (rank == 2) {
        // gFarm.add_emitter(new ff_dAreceiver(new ff_dCommTCP(g2, true, {2,3}, {{1,0}}, {"G1"}), 2, -1, 1));      //TCP
        // gFarm.add_collector(new ff_dAsender(new ff_dCommTCPS({g1, g3}, "G2", {"G1"})));                         //TCP

        gFarm.add_emitter(new ff_dAreceiver(new ff_dCommMPI(g2, true, {2,3}, {{1,0}}, {"G1"}, {1}), 2, -1, 1, (ntask/2)+ntask));      //MPI
        gFarm.add_collector(new ff_dAsender(new ff_dCommMPIS({g1, g3}, "G2", {"G1"}, {1})));                         //MPI

		gFarm.cleanup_emitter();
		gFarm.cleanup_collector();

		auto s = new Lnode(4,1,Lmswait);                                                                           
		auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF)), new EmitterAdapter(s, 4, 1, {{2,0}, {3,1}}, true), true, true);

        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({std::make_pair(2,0), std::make_pair(3,1)})}, 0, true);

        auto rnode2 = new Rnode(2, Rmswait);
		auto rnode3 = new Rnode(3, Rmswait);
		a2a.add_secondset<ff_node>({
									new ff_comb(new CollectorAdapter(rnode2, {1}, true),
												new WrapperOUT(new ForwarderNode(rnode2->serializeF), 2, 1, true), true, true),
									new ff_comb(new CollectorAdapter(rnode3, {1}, true),
												new WrapperOUT(new ForwarderNode(rnode3->serializeF), 3, 1, true), true, true),
									new SquareBoxRight   // this box should be the last one!
			                        }, true);		
        
    } else {

        std::string *test_type;
        test_type = new std::string("TCP");

        printf("-- Testing %s communication with \"%s\" --\n", test_type->c_str(), protocol);
        int total_task = ntask * 4;
        int expected_completion = std::max((ntask/2) * Lmswait, ntask * Rmswait);
        
        printf("Configuration || ntask: %d - LNode wait (ms per task): %d - RNode wait (ms per task): %d\n", ntask, Lmswait, Rmswait);
        printf("Total number of task to the Sink node: %d\n", total_task);
        printf("Expected completion time (in ms): %d\n", expected_completion);

        ffTime(START_TIME);
        
        // gFarm.add_emitter(new ff_dAreceiver(new ff_dCommTCP(g3, false),2, -1, 1));                      
        gFarm.add_emitter(new ff_dAreceiver(new ff_dCommMPI(g3, false),2, -1, 1, ntask*4));                      
		
		gFarm.add_workers({new WrapperIN(new Sink(verbose), 1, true)});
        gFarm.run_and_wait_end();
        
        ffTime(STOP_TIME);
        std::cout << "Time: " << ffTime(GET_TIME) << "\n";
        MPI_Finalize();
		return 0;
    }

    gFarm.add_workers({&a2a});
    gFarm.run_and_wait_end();

    MPI_Finalize();
	return 0;
}
