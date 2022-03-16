#include <iostream>

#include <ff/ff.hpp>
#include <margo.h>
#include <abt.h>

#include "my-rpc.h"

using namespace ff;

void ff_rpc(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc);

void ff_rpc_shutdown(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc_shutdown);



// Margo communicator node (server)
struct receiverStage: ff_node_t<float> {
    hg_return_t         hret;
    margo_instance_id   mid1, mid2;
    hg_id_t             id, id2, ff_shutdown_id1, ff_shutdown_id2;
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

    receiverStage(char* addr1, char* addr2) : a1{addr1}, a2{addr2}, num_rpc1{0}, num_rpc2{0} {
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

        ff_shutdown_id1 = MARGO_REGISTER_PROVIDER(mid1, "ff_rpc_shutdown", void, void, ff_rpc_shutdown, MARGO_DEFAULT_PROVIDER_ID, ABT_POOL_NULL);
        margo_registered_disable_response(mid1, ff_shutdown_id1, HG_TRUE);

        id2 = MARGO_REGISTER_PROVIDER(mid2, "ff_rpc", ff_rpc_in_t, void, ff_rpc, MARGO_DEFAULT_PROVIDER_ID, ABT_POOL_NULL);
        margo_registered_disable_response(mid2, id2, HG_TRUE);
        margo_info(mid2, "id: %d\n", id2);
        margo_register_data(mid2, id2, this, NULL);

        ff_shutdown_id2 = MARGO_REGISTER_PROVIDER(mid2, "ff_rpc_shutdown", void, void, ff_rpc_shutdown, MARGO_DEFAULT_PROVIDER_ID, ABT_POOL_NULL);
        margo_registered_disable_response(mid2, ff_shutdown_id2, HG_TRUE);

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

// Margo communicator node (client)
class senderStage: public ff_node_t<float> {

private:
    char*                   addr;
    margo_instance_id       mid;
    hg_addr_t               svr_addr;
    hg_id_t                 ff_rpc_id, ff_shutdown_id;
    ABT_pool                pool_e1;
    ABT_xstream             xstream_e1;
    struct margo_init_info  args_e1;

public:
    senderStage(char* addr) : addr{addr}, svr_addr{HG_ADDR_NULL} {
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
            // FIXME: We must have a way to manage wrong allocation of mid class
            // return -1;
        }
        margo_set_log_level(mid, MARGO_LOG_TRACE);
        free(proto);
        
        
        /* register RPC */
        ff_rpc_id = MARGO_REGISTER(mid, "ff_rpc", ff_rpc_in_t, void, NULL);
        margo_registered_disable_response(mid, ff_rpc_id, HG_TRUE);
        margo_addr_lookup(mid, addr, &svr_addr);

        ff_shutdown_id = MARGO_REGISTER_PROVIDER(mid, "ff_rpc_shutdown", void, void, NULL, MARGO_DEFAULT_PROVIDER_ID, ABT_POOL_NULL);
        margo_registered_disable_response(mid, ff_shutdown_id, HG_TRUE);
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

    void svc_end() {
        std::cout << "Finalizing...\n";
        
        hg_handle_t h;
        printf("Got EOS\n");
        margo_create(mid, svr_addr, ff_shutdown_id, &h);
        margo_forward(h, NULL);
        margo_destroy(h);
        
        margo_finalize(mid);
        finalize_xstream_cb(xstream_e1);
        ABT_xstream_state state;
        ABT_xstream_get_state(xstream_e1, &state);
        std::cout << "State: " << state << "\n";
        ABT_pool_free(&pool_e1);
    }
};


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

    receiverStage* my_first = (receiverStage*)margo_registered_data(mid, info->id);

    my_first->ff_send_out(new float(*in.task));

    margo_free_input(handle, &in);
    margo_destroy(handle);
    
    // my_first->num_rpc1++;
    // if(my_first->num_rpc1 >= 20)
    //     margo_finalize(mid);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc)


void ff_rpc_shutdown(hg_handle_t handle)
{
    hg_return_t       hret;
    margo_instance_id mid;

    printf("Got RPC request to shutdown\n");

    /* get handle info and margo instance */
    mid = margo_hg_handle_get_instance(handle);
    assert(mid != MARGO_INSTANCE_NULL);

    margo_destroy(handle);
    margo_finalize(mid);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown)