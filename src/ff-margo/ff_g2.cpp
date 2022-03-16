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


// Normal FF stage
struct secondStage: ff_node_t<float> {
    // secondStage(){}
    float* svc(float *task) {
        std::cout << "Received: " << *task << "\n"; 
        std::this_thread::sleep_for (std::chrono::seconds(1));
        return task;
    }
};

// Normal FF stage
struct testStage: ff_node_t<float> {
    testStage(const size_t length):length(length) {}
    float* svc(float *) {
        for(size_t i=0; i<length; ++i) {
            ff_send_out(new float(i+10));
            std::cout << "Sent out to next stage: " << i << "\n";
        }
        return EOS;
    }
    const size_t length;
};

int main(int argc, char** argv)
{
    if (argc != 4) {
        fprintf(stderr, "Usage: ./server <listen_addr1> <listen_addr2> <contact address>\n");
        fprintf(stderr, "Example: ./server na+sm:// ofi+tcp:// ofi+sockets://\n");
        return (-1);
    }

    // margo_set_global_log_level(MARGO_LOG_TRACE);

    // NOTE: In order to allow different threads to use the same ABT
    // initialization we need to initialize the mid classes in the constructor.
    // This meaning that different stages are strictly related to the ABT_init
    // below.

    margo_set_environment(NULL);
    ABT_init(0, NULL);

    secondStage second;

    receiverStage* first = new receiverStage(argv[1], argv[2]);
    senderStage third(argv[3]);
    ff_Pipe<float> pipe(first, second, third);
    if (pipe.run_and_wait_end()<0) {
        error("running pipe");
        return -1;
    }

    ABT_finalize();

    return (0);
}