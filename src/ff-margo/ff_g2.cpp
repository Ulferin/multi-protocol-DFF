/**
 * @file ff_g2.cpp
 * @author Federico Finocchio
 * @brief Second group for testing Margo library as a communication layer in FF.
 * The second group is composed as follow:
 *             _______________________
 *            |                       |
 * --remote---|--> S1 --> S2 --> S3 --|-remote-->
 *            |_______________________|
 * 
 * Where the remote connections are handled via Margo RPC calls and the internal
 * stage connections are usual FF shared memory connections.
 * The pipeline stages are FastFlow nodes, where S1 is a "receiver" node, which
 * waits for incoming RPC calls and forward the arguments to the second stage.
 * The last stage (S3) gets the stream elements from S2 and forwards them to an
 * attached remote group via RPC requests.
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

#include <ff/ff.hpp>
#include <ff/pipeline.hpp>

#include "my-rpc.h"

using namespace ff;

void ff_rpc(hg_handle_t handle);
hg_return_t _handler_for_ff_rpc(hg_handle_t handle);


static void finalize_xstream_cb(void* data) {
    ABT_xstream xstream = (ABT_xstream)data;
    ABT_xstream_join(xstream);
    ABT_xstream_free(&xstream);
}


// Margo communicator node (server)
struct firstStage: ff_node_t<float> {
    hg_return_t       hret;
    margo_instance_id mid1, mid2;
    hg_id_t           id, id2;
    hg_addr_t         addr_self;
    char              addr_self_string1[128];
    char              addr_self_string2[128];
    hg_size_t         addr_self_string_sz = 128;

    char*             a1;
    char*             a2;

    firstStage(char* addr1, char* addr2) : a1{addr1}, a2{addr2} {}

    friend hg_return_t _handler_for_ff_rpc(hg_handle_t h);
    friend void ff_rpc(hg_handle_t handle);

    int svc_init() {

        /***************************************/
        mid1 = margo_init(a1, MARGO_SERVER_MODE, 1, -1);
        if (mid1 == MARGO_INSTANCE_NULL) {
            fprintf(stderr, "Error: margo_init()\n");
            return (-1);
        }
        margo_set_log_level(mid1, MARGO_LOG_INFO);

        mid2 = margo_init(a2, MARGO_SERVER_MODE, 1, -1);
        if (mid2 == MARGO_INSTANCE_NULL) {
            fprintf(stderr, "Error: margo_init()\n");
            return (-1);
        }
        margo_set_log_level(mid2, MARGO_LOG_INFO);

        /* figure out first listening addr */
        hret = margo_addr_self(mid1, &addr_self);
        if (hret != HG_SUCCESS) {
            fprintf(stderr, "Error: margo_addr_self()\n");
            margo_finalize(mid1);
            return (-1);
        }
        addr_self_string_sz = 128;
        hret = margo_addr_to_string(mid1, addr_self_string1, &addr_self_string_sz,
                                    addr_self);
        if (hret != HG_SUCCESS) {
            fprintf(stderr, "Error: margo_addr_to_string()\n");
            margo_addr_free(mid1, addr_self);
            margo_finalize(mid1);
            return (-1);
        }
        margo_addr_free(mid1, addr_self);

        /* figure out second listening addr */
        hret = margo_addr_self(mid2, &addr_self);
        if (hret != HG_SUCCESS) {
            fprintf(stderr, "Error: margo_addr_self()\n");
            margo_finalize(mid2);
            return (-1);
        }
        addr_self_string_sz = 128;
        hret = margo_addr_to_string(mid2, addr_self_string2, &addr_self_string_sz,
                                    addr_self);
        if (hret != HG_SUCCESS) {
            fprintf(stderr, "Error: margo_addr_to_string()\n");
            margo_addr_free(mid2, addr_self);
            margo_finalize(mid2);
            return (-1);
        }
        margo_addr_free(mid2, addr_self);
        fprintf(stderr, "# accepting RPCs on address \"%s\" and \"%s\"\n",
                addr_self_string1, addr_self_string2);

        /* register RPC */
        id = MARGO_REGISTER_PROVIDER(mid1, "ff_rpc", ff_rpc_in_t, void, ff_rpc, MARGO_DEFAULT_PROVIDER_ID, ABT_POOL_NULL);
        margo_registered_disable_response(mid1, id, HG_TRUE);
        margo_info(mid1, "id: %d\n", id);
        margo_register_data(mid1, id, this, NULL);

        id = MARGO_REGISTER_PROVIDER(mid2, "ff_rpc", ff_rpc_in_t, void, ff_rpc, MARGO_DEFAULT_PROVIDER_ID, ABT_POOL_NULL);
        margo_registered_disable_response(mid2, id2, HG_TRUE);
        margo_register_data(mid2, id2, this, NULL);

        return 0;
    }

    float* svc(float * task) { 
        /* NOTE: there isn't anything else for the server to do at this point
        * except wait for itself to be shut down.  The
        * margo_wait_for_finalize() call here yields to let Margo drive
        * progress until that happens.
        */
        margo_wait_for_finalize(mid2);
        margo_wait_for_finalize(mid1);

        return EOS;
    }
};


// Normal FF stage
struct secondStage: ff_node_t<float> {
    // secondStage(){}
    float* svc(float *task) {
        std::cout << "Received: " << *task << "\n"; 
        delete task;

        return GO_ON;
    }
};

struct thirdStage: ff_node_t<float> {

    char*                   addr;
    margo_instance_id       mid;
    hg_addr_t               svr_addr;
    hg_id_t                 ff_rpc_id;


    thirdStage(char* addr) : addr{addr}, svr_addr{HG_ADDR_NULL} {}

    int svc_init() {
        int                    i;
        int                    ret;
        hg_return_t            hret;
        hg_handle_t            handle;

        /* initialize Mercury using the transport portion of the destination
        * address (i.e., the part before the first : character if present)
        */

        mid = margo_init(addr, MARGO_CLIENT_MODE, 0, 0);
        std::cout << "Prova: " << addr << "\n";
        margo_set_log_level(mid, MARGO_LOG_INFO);
        margo_info(mid, "registering");

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

firstStage* first;
int main(int argc, char** argv)
{
    if (argc != 4) {
        fprintf(stderr, "Usage: ./server <listen_addr1> <listen_addr2> <third address>\n");
        fprintf(stderr, "Example: ./server na+sm:// ofi+tcp:// ofi+sockets://\n");
        return (-1);
    }

    first = new firstStage(argv[1], argv[2]);
    secondStage second;
    thirdStage third(argv[3]);
    ff_Pipe<float> pipe(first, second);
    if (pipe.run_and_wait_end()<0) {
        error("running pipe");
        return -1;
    }
    
    return (0);
}

void ff_rpc(hg_handle_t handle)
{
    hg_return_t           hret;
    ff_rpc_in_t           in;
    hg_size_t             size;
    const struct hg_info* hgi;
    margo_instance_id mid;

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);
    /* get handle info and margo instance */
    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);
    assert(mid != MARGO_INSTANCE_NULL);

    // margo_info(mid, "Got RPC request with input_val: %f\n", *in.task);
    const struct hg_info* info = margo_get_info(handle);

    firstStage* my_first = (firstStage*)margo_registered_data(mid, info->id);

    first->ff_send_out(new float(*in.task));

    margo_free_input(handle, &in);
    margo_destroy(handle);
    // margo_finalize(mid);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc)

