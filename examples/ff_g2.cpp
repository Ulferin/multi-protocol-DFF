/**
 * @file ff_g2.cpp
 * @author Federico Finocchio
 * @brief Second group for testing Margo library as a communication layer in FF.
 * The second group (G2) is composed as described in the picture below:
 *  ___________             __________________              ___________
 * |           |           |                  |            |           |
 * | ff_g1.out |--remote--|R|--> S1 --> S2 --|S|--remote-->| ff_g3.out |
 * |___________|           |__________________|            |___________|
 *      G1                           G2                         G3
 * 
 * Remote connections are handled via Margo RPC calls and the internal
 * stage connections are usual FF shared memory connections.
 * 
 * The receiverStage (R) listens for incoming RPCs on a list of endpoints,
 * potentially using different protocols and port numbers. They must be
 * specified during initilization. (S1) and (S2) nodes are just "dummy" nodes
 * which simulate work and forward stream elements to the senderStage (S).
 * The last stage (S) gets the stream elements from S2 and forwards them to an
 * attached remote group (G3) via RPC requests. Remote endpoint address must be
 * specified upon initialization.
 * 
 * 
 * @date 2022-03-16
 * 
 * 
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <mercury.h>
#include <iostream>
#include <thread>

#include <abt.h>
#include <margo.h>
#include <ff/ff.hpp>
#include <ff/pipeline.hpp>

#include "my-rpc.h"
#include "ff_comm.hpp"

using namespace ff;

#define SLEEP_FIRST 0
#define SLEEP_SECOND 0

struct firstStage: ff_node_t<float> {
    float* svc(float * task) { 
        auto &t = *task; 

        std::this_thread::sleep_for(std::chrono::seconds(SLEEP_FIRST));       
        t = t*t;
        std::cout << "[FIRST]sending out: " << t << "\n";
        return task; 
    }
};

struct secondStage: ff_node_t<float> {
    float* svc(float * task) { 
        auto &t = *task; 

        std::this_thread::sleep_for(std::chrono::seconds(SLEEP_SECOND));       
        t = t+1;
        std::cout << "[SECOND]sending out: " << t << "\n";
        return task; 
    }
};


int main(int argc, char** argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: ./server <busy mode> <remote address> <listen_addr 1> ..."\
                " <listen_addr n>\n");
        fprintf(stderr, "You should provide the address to connect this group"\
                " with (remote address) "\
        "and at least one address which this group will listen on.\n");
        fprintf(stderr, "Example: ./server 0 ofi+sockets://1.2.3.4:1234"\
                " na+sm:// ofi+tcp://\n");
        return -1;
    }


    margo_set_environment(NULL);
    // margo_set_global_log_level(MARGO_LOG_TRACE);
    ABT_init(0, NULL);

    int busy = std::atoi(argv[1]);

    // NOTE: here most likely we will have a vector/map composed of addresses
    //       coming from a configuration file
    std::vector<char*> addresses;
    for (int i = 3; i < argc; i++)
    {
        addresses.push_back(argv[i]);
    }

    // Build the pipe and run
    receiverStage receiver(addresses, busy);
    firstStage first;
    secondStage second;
    senderStage sender(argv[2], busy);
    ff_Pipe<float> pipe(receiver, first, second, sender);

    if (pipe.run_and_wait_end()<0) {
        error("running pipe");
        return -1;
    }
    std::cout << "Time: " << pipe.ffTime() << "\n";

    ABT_finalize();
    return (0);
}