/**
 * @file ff_g2.cpp
 * @author Federico Finocchio
 * @brief Second group for testing Margo library as a communication layer in FF.
 * The second group (G2) is composed as described in the picture below:
 *  ___________             __________________             ___________
 * |           |           |                  |           |           |
 * | ff_g1.out |--remote-->| S1 --> S2 --> S3 |--remote-->| ff_g3.out |
 * |___________|           |__________________|           |___________|
 *      G1                           G2                         G3
 * 
 * Remote connections are handled via Margo RPC calls and the internal
 * stage connections are usual FF shared memory connections.
 * 
 * The pipeline stages are FastFlow nodes, where (S1) is a "receiver" node,
 * which waits for incoming RPC calls and forward the arguments to the (S2).
 * In this particular example (S1) listens for incoming RPCs on two endpoints,
 * potentially using different protocols and port numbers. They must be
 * specified during initilization. (S2) node is just a forwarder between the
 * two extremes. The last stage (S3) gets the stream elements from S2 and
 * forwards them to an attached remote group (G3) via RPC requests.
 * 
 * 
 * @date 2022-03-10
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

#define SLEEP_FIRST 2
#define SLEEP_SECOND 3

struct firstStage: ff_node_t<float> {
    float* svc(float * task) { 
        auto &t = *task; 

        std::this_thread::sleep_for(std::chrono::seconds(SLEEP_FIRST));       
        t = t*t;
        std::cout << "First stage out: " << t << "\n";
        return task; 
    }
};

struct secondStage: ff_node_t<float> {
    float* svc(float * task) { 
        auto &t = *task; 

        std::this_thread::sleep_for(std::chrono::seconds(SLEEP_SECOND));       
        t = t+1;
        std::cout << "Second stage out: " << t << "\n";
        return task; 
    }
};


int main(int argc, char** argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: ./server <remote address> <listen_addr 1> ... <listen_addr n>\n");
        fprintf(stderr, "You should provide the address to connect this group with (remote address) "\
        "and at least one address which this group will listen on.\n");
        fprintf(stderr, "Example: ./server ofi+sockets://1.2.3.4:1234 na+sm:// ofi+tcp://\n");
        return (-1);
    }

    // TODO: check this claim
    // NOTE: In order to allow different threads to use the same ABT
    // initialization we need to initialize the mid classes in the constructor.
    // This meaning that different stages are strictly related to the ABT_init
    // below.

    margo_set_environment(NULL);
    // margo_set_global_log_level(MARGO_LOG_TRACE);
    ABT_init(0, NULL);

    // NOTE: here most likely we will have a vector/map composed of addresses
    //       coming from a configuration file
    std::vector<char*>* addresses = new std::vector<char*>();
    for (int i = 2; i < argc; i++)
    {
        (*addresses).push_back(argv[i]);
    }

    receiverStage receiver(addresses);
    firstStage first;
    secondStage second;
    senderStage sender(argv[1]);
    ff_Pipe<float> pipe(receiver, first, second, sender);
    if (pipe.run_and_wait_end()<0) {
        error("running pipe");
        return -1;
    }
    std::cout << "Time: " << pipe.ffTime() << "\n";
    ABT_finalize();

    return (0);
}