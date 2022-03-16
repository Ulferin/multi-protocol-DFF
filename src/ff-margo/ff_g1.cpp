/**
 * @file ff_g1.cpp
 * @author Federico Finocchio
 * @brief First group for testing Margo library as a communication layer in FF.
 * The first group (G1) is composed as described in the picture below:
 *  ___________             ___________             ___________
 * |           |           |           |           |           |
 * | S1 --> S2 |--remote-->| ff_g2.out |--remote-->| ff_g3.out |
 * |___________|           |___________|           |___________|
 *      G1                       G2                      G3
 * 
 * Remote connections are handled via Margo RPC calls and the internal
 * stage connections are usual FF shared memory connections.
 * 
 * The pipeline stages are FF nodes, (S1) represents the stream (endo-stream)
 * generator, which forwards task to (S2). (S2), in turns, issues RPC calls to
 * the G2 group. G2 may be using multiple endpoints to handle the requests, so
 * the address must be specified when initializing (S2).
 * 
 * @date 2022-03-16
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include <math.h>
#include <iostream>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <mercury.h>
#include <abt.h>
#include <margo.h>

#include <ff/ff.hpp>
#include <ff/pipeline.hpp>

#include "my-rpc.h"
#include "ff_comm.hpp"

using namespace ff;


// Normal FF stage
struct firstStage: ff_node_t<float> {
    firstStage(const size_t length):length(length) {}
    float* svc(float *) {
        for(size_t i=0; i<length; ++i) {
            ff_send_out(new float(i));
            std::cout << "Sent out to next stage: " << i << "\n";
        }
        return EOS;
    }
    const size_t length;
};


int main(int argc, char** argv)
{

    if(argc != 3) {
        std::cout << "Usage: " << argv[0] << " <stream len> <remote addr>\n";
        return 1;
    }

    margo_set_environment(NULL);
    margo_set_global_log_level(MARGO_LOG_TRACE);
    ABT_init(0, NULL);

    firstStage  first(std::stol(argv[1]));
    senderStage second(argv[2]);
    ff_Pipe<float> pipe(first, second);
    if (pipe.run_and_wait_end()<0) {
        error("running pipe");
        return -1;
    }

    ABT_finalize();
}
