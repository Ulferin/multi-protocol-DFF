/**
 * @file ff_g3.cpp
 * @author Federico Finocchio
 * @brief Third group for testing Margo library as a communication layer in FF.
 * The third group (G3) is composed as described in the picture below:
 *  ___________             ___________             ___________
 * |           |           |           |           |           |
 * | ff_g1.out |--remote-->| ff_g2.out |--remote-->| S1 --> S2 |
 * |___________|           |___________|           |___________|
 *      G1                       G2                      G3
 * 
 * Remote connections are handled via Margo RPC calls and the internal
 * stage connections are usual FF shared memory connections.
 * 
 * The pipeline stages are FastFlow nodes, where (S1) is a "receiver" node,
 * which waits for incoming RPC calls and forward the arguments to the (S2).
 * In this particular example (S1) listens for incoming RPCs on two endpoints,
 * potentially using different protocols and port numbers. They must be
 * specified during initilization. (S2) node is just a forwarder, it ignores
 * the received tasks.
 * 
 * 
 * @date 2022-03-16
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <mercury.h>
#include <abt.h>
#include <margo.h>

#include <iostream>
#include <thread>

#include <ff/ff.hpp>
#include <ff/pipeline.hpp>

#include "my-rpc.h"
#include "ff_comm.hpp"

using namespace ff;

#define SLEEP_FIRST 0

struct firstStage: ff_node_t<float> {   
    float* svc(float * task) { 
        auto &t = *task;
        sum += t; 
        delete task;
        std::this_thread::sleep_for(std::chrono::seconds(SLEEP_FIRST));       
        return GO_ON; 
    }
    void svc_end() { std::cout << "sum = " << sum << "\n"; }
    float sum{0.0};
};


int main(int argc, char** argv)
{
    if (argc < 2) {
        fprintf(stderr, "Usage: ./server <listen_addr1> <listen_addr2>\n");
        fprintf(stderr, "Example: ./server na+sm:// ofi+tcp://\n");
        return (-1);
    }

    margo_set_environment(NULL);
    margo_set_global_log_level(MARGO_LOG_TRACE);
    ABT_init(0, NULL);

    std::vector<char*>* addresses = new std::vector<char*>();
    for (int i = 1; i < argc; i++)
    {
        (*addresses).push_back(argv[i]);
    }

    receiverStage receiver(addresses);
    firstStage first;
    ff_Pipe<float> pipe(receiver, first);
    if (pipe.run_and_wait_end()<0) {
        error("running pipe");
        return -1;
    }
    std::cout << "Time: " << pipe.ffTime() << "\n";
    ABT_finalize();

    return (0);
}