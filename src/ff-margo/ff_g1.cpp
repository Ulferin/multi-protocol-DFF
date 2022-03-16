/**
 * @file ff_g1.cpp
 * @author Federico Finocchio
 * @brief First group for testing Margo library as a communication layer in FF.
 * The first group (G1) is composed as follow:
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

using namespace ff;


// Normal FF stage
struct firstStage: ff_node_t<float> {
    firstStage(const size_t length):length(length) {}
    float* svc(float *) {
        for(size_t i=0; i<length; ++i) {
            ff_send_out(new float(i+10));
            std::cout << "Sent out to next stage: " << i << "\n";
        }
        return EOS;
    }
    const size_t length;
};

// Margo communicator node (client)
class secondStage: public ff_node_t<float> {

private:
    char*                   addr;
    margo_instance_id       mid;
    hg_addr_t               svr_addr;
    hg_id_t                 ff_rpc_id;


public:
    secondStage(char* addr) : addr{addr}, svr_addr{HG_ADDR_NULL} {}

    int svc_init() {
        int                    i;
        int                    ret;
        hg_return_t            hret;
        hg_handle_t            handle;
        char*                  proto;
        char*                  colon;

        /* initialize Mercury using the transport portion of the destination
        * address (i.e., the part before the first : character if present)
        */
        proto = strdup(addr);
        assert(proto);
        colon = strchr(proto, ':');
        if (colon) *colon = '\0';

        mid = margo_init(proto, MARGO_CLIENT_MODE, 0, 0);
        margo_set_log_level(mid, MARGO_LOG_INFO);
        free(proto);
        if (mid == MARGO_INSTANCE_NULL) {
            fprintf(stderr, "Error: margo_init_ext()\n");
            return (-1);
        }
        
        /* register RPC */
        ff_rpc_id = MARGO_REGISTER(mid, "ff_rpc", ff_rpc_in_t, void, NULL);
        margo_registered_disable_response(mid, ff_rpc_id, HG_TRUE);
        margo_addr_lookup(mid, addr, &svr_addr);
        
        return (0);
    }

    float* svc(float * task) { 
        auto &t = *task; 
        ff_rpc_in_t in;
        hg_return_t ret;
        hg_handle_t h;

        in.task = new float(*task);
        delete task;

        std::cout << "Sending out: " << *in.task << "\n";
        margo_create(mid, svr_addr, ff_rpc_id, &h);
        margo_forward(h, &in);

        margo_destroy(h);

        return GO_ON;
    }
};


int main(int argc, char** argv)
{

    if(argc != 3) {
        std::cout << "Usage: " << argv[0] << " <stream len> <remote addr>\n";
        return 1;
    }

    firstStage  first(std::stol(argv[1]));
    secondStage second(argv[2]);
    ff_Pipe<float> pipe(first, second);
    if (pipe.run_and_wait_end()<0) {
        error("running pipe");
        return -1;
    }
}
