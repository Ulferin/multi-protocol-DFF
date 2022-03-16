/**
 * @file ff_g2.cpp
 * @author Federico Finocchio
 * @brief Second group for testing Margo library as a communication layer in FF.
 * The second group (G2) is composed as follow:
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

using namespace ff;

void ff_rpc(hg_handle_t handle);
hg_return_t _handler_for_ff_rpc(hg_handle_t handle);

static void wait_fin(void* arg) {
    margo_instance_id* mid = (margo_instance_id*)arg;
    margo_wait_for_finalize(*mid);
}

static void finalize_xstream_cb(void* data) {
    ABT_xstream xstream = (ABT_xstream)data;
    ABT_xstream_join(xstream);
    ABT_xstream_free(&xstream);
}

// Margo communicator node (server)
struct firstStage: ff_node_t<float> {
    hg_return_t         hret;
    margo_instance_id   mid1, mid2;
    hg_id_t             id, id2;
    hg_addr_t           addr_self;
    char                addr_self_string1[128];
    char                addr_self_string2[128];
    hg_size_t           addr_self_string_sz = 128;

    ABT_pool            pool_e1, pool_e2, pool_wait;
    ABT_xstream         xstream_e1, xstream_e2, xstream_wait;

    char*               a1;
    char*               a2;

    int                 num_rpc1, num_rpc2;

    struct margo_init_info args_e1, args_e2;

    firstStage(char* addr1, char* addr2) : a1{addr1}, a2{addr2}, num_rpc1{0}, num_rpc2{0} {
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC, ABT_FALSE, &pool_e1);
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC, ABT_FALSE, &pool_e2);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1, ABT_SCHED_CONFIG_NULL, &xstream_e1);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e2, ABT_SCHED_CONFIG_NULL, &xstream_e2);

        args_e1 = {
            .json_config   = NULL,      /* const char*          */
            .progress_pool = pool_e1, /* ABT_pool             */
            .rpc_pool      = pool_e1, /* ABT_pool             */
            .hg_class      = NULL,      /* hg_class_t*          */
            .hg_context    = NULL,      /* hg_context_t*        */
            .hg_init_info  = NULL       /* struct hg_init_info* */
        };

        args_e2 = {
            .json_config   = NULL,      /* const char*          */
            .progress_pool = pool_e2, /* ABT_pool             */
            .rpc_pool      = pool_e2, /* ABT_pool             */
            .hg_class      = NULL,      /* hg_class_t*          */
            .hg_context    = NULL,      /* hg_context_t*        */
            .hg_init_info  = NULL       /* struct hg_init_info* */
        };

        /***************************************/
        mid1 = margo_init_ext(a1, MARGO_SERVER_MODE, &args_e1);
        if (mid1 == MARGO_INSTANCE_NULL) {
            fprintf(stderr, "Error: margo_init()\n");
        }
        
        mid2 = margo_init_ext(a2, MARGO_SERVER_MODE, &args_e2);
        if (mid2 == MARGO_INSTANCE_NULL) {
            fprintf(stderr, "Error: margo_init()\n");
        }

#ifdef DEBUG
        margo_set_log_level(mid1, MARGO_LOG_TRACE);
        char* config = margo_get_config(mid1);
        margo_info(mid1, "%s", config);
        free(config);

        margo_set_log_level(mid2, MARGO_LOG_TRACE);
        char* config1 = margo_get_config(mid2);
        margo_info(mid2, "%s", config1);
        free(config1);  
#endif

        get_self_addr(mid1, addr_self_string1);
        get_self_addr(mid2, addr_self_string2);
        fprintf(stderr, "# accepting RPCs on address \"%s\" and \"%s\"\n",
                addr_self_string1, addr_self_string2);

        /* register RPC */
        id = MARGO_REGISTER_PROVIDER(mid1, "ff_rpc", ff_rpc_in_t, void, ff_rpc, MARGO_DEFAULT_PROVIDER_ID, ABT_POOL_NULL);
        margo_registered_disable_response(mid1, id, HG_TRUE);
        margo_info(mid1, "id: %d\n", id);
        margo_register_data(mid1, id, this, NULL);

        id2 = MARGO_REGISTER_PROVIDER(mid2, "ff_rpc", ff_rpc_in_t, void, ff_rpc, MARGO_DEFAULT_PROVIDER_ID, ABT_POOL_NULL);
        margo_registered_disable_response(mid2, id2, HG_TRUE);
        margo_info(mid2, "id: %d\n", id2);
        margo_register_data(mid2, id2, this, NULL);

    }

    friend hg_return_t _handler_for_ff_rpc(hg_handle_t h);
    friend void ff_rpc(hg_handle_t handle);

    float* svc(float * task) {
        ABT_thread t_e1, t_e2;
        ABT_thread_create(pool_e1, wait_fin, &mid1, NULL, &t_e1);
        ABT_thread_create(pool_e2, wait_fin, &mid2, NULL, &t_e2);

        finalize_xstream_cb(xstream_e1);
        finalize_xstream_cb(xstream_e2);
        ABT_pool_free(&pool_e1);
        ABT_pool_free(&pool_e2);
        return EOS;
    }
};


// Normal FF stage
struct secondStage: ff_node_t<float> {
    // secondStage(){}
    float* svc(float *task) {
        std::cout << "Received: " << *task << "\n"; 

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


// Margo communicator node (client)
class thirdStage: public ff_node_t<float> {

private:
    char*                   addr;
    margo_instance_id       mid;
    hg_addr_t               svr_addr;
    hg_id_t                 ff_rpc_id;
    ABT_pool                pool_e1;
    ABT_xstream             xstream_e1;
    struct margo_init_info  args_e1;

public:
    thirdStage(char* addr) : addr{addr}, svr_addr{HG_ADDR_NULL} {
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

        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC, ABT_FALSE, &pool_e1);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1, ABT_SCHED_CONFIG_NULL, &xstream_e1);

        args_e1 = {
            .json_config   = NULL,      /* const char*          */
            .progress_pool = pool_e1, /* ABT_pool             */
            .rpc_pool      = pool_e1, /* ABT_pool             */
            .hg_class      = NULL,      /* hg_class_t*          */
            .hg_context    = NULL,      /* hg_context_t*        */
            .hg_init_info  = NULL       /* struct hg_init_info* */
        };

        mid = margo_init_ext(proto, MARGO_CLIENT_MODE, &args_e1);
        // printf("After mid_init\n");
        if (mid == MARGO_INSTANCE_NULL) {
            fprintf(stderr, "Error: margo_init_ext()\n");
            // FIXME: We have a way to manage wrong allocation of mid class
            // return -1;
        }
        margo_set_log_level(mid, MARGO_LOG_INFO);
        free(proto);
        
        
        /* register RPC */
        ff_rpc_id = MARGO_REGISTER(mid, "ff_rpc", ff_rpc_in_t, void, NULL);
        margo_registered_disable_response(mid, ff_rpc_id, HG_TRUE);
        margo_addr_lookup(mid, addr, &svr_addr);
    }

    float* svc(float * task) {
        printf("Got a task!\n");
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

    firstStage* first = new firstStage(argv[1], argv[2]);
    secondStage second;
    testStage test(10);
    thirdStage third(argv[3]);
    ff_Pipe<float> pipe(first, second, third);
    if (pipe.run_and_wait_end()<0) {
        error("running pipe");
        return -1;
    }
    // TODO: add xstream join+free
    ABT_finalize();

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

    my_first->ff_send_out(new float(*in.task));

    margo_free_input(handle, &in);
    margo_destroy(handle);
    
    // my_first->num_rpc1++;
    // if(my_first->num_rpc1 >= 20)
    //     margo_finalize(mid);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc)