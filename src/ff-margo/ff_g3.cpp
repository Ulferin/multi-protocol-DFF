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

#include <ff/ff.hpp>
#include <ff/pipeline.hpp>

#include "my-rpc.h"
#include "ff_comm.hpp"

using namespace ff;


// Normal FF stage
struct secondStage: ff_node_t<float> {
    // secondStage(){}
    float* svc(float *task) {
        std::cout << "Received: " << *task << "\n"; 

        return GO_ON;
    }
};


int main(int argc, char** argv)
{
    if (argc != 3) {
        fprintf(stderr, "Usage: ./server <listen_addr1> <listen_addr2>\n");
        fprintf(stderr, "Example: ./server na+sm:// ofi+tcp://\n");
        return (-1);
    }
    margo_set_environment(NULL);
    margo_set_global_log_level(MARGO_LOG_TRACE);
    ABT_init(0, NULL);

    receiverStage* first = new receiverStage(argv[1], argv[2]);
    secondStage second;
    ff_Pipe<float> pipe(first, second);
    if (pipe.run_and_wait_end()<0) {
        error("running pipe");
        return -1;
    }

    ABT_finalize();

    return (0);
}