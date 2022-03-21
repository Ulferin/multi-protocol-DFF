#include <iostream>

#include <ff/ff.hpp>
#include <margo.h>
#include <vector>
#include <abt.h>

#include "my-rpc.h"

using namespace ff;

void ff_rpc(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc);

void ff_rpc_shutdown(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc_shutdown);



// Margo communicator node (server)
class receiverStage: public ff_node_t<float> {
public:
    hg_return_t         hret;
    std::vector<margo_instance_id*>* mids;
    
    hg_addr_t           addr_self;
    ABT_pool            pool_e1;
    ABT_xstream         xstream_e1;

    receiverStage(std::vector<char*>* addresses) {
        // NOTE: amount of pools and ES can be tweaked however we want here, as
        //       as long as we keep the waiting calls in different ULTs, since
        //       they are blocking calls and would block the whole main ULT in
        //       case we do not separate them.

        mids = new std::vector<margo_instance_id*>();
        // TODO: pools and xstreams can be placed in a vector
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC, ABT_FALSE, &pool_e1);
        // ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC, ABT_FALSE, &pool_e2);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1, ABT_SCHED_CONFIG_NULL, &xstream_e1);
        // ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e2, ABT_SCHED_CONFIG_NULL, &xstream_e2);    


        for (auto &&addr : *addresses)
        {
            margo_instance_id* mid = new margo_instance_id();
            init_mid(addr, mid);
            (*mids).push_back(mid);
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

        for (auto &&mid : *mids)
        {
            char addr_self_string[128];
            get_self_addr(mid, addr_self_string);

            fprintf(stderr, "# accepting RPCs on address \"%s\"\n",
                addr_self_string);

            register_rpcs(mid);
        }

    }

    void register_rpcs(margo_instance_id* mid) {
        hg_id_t id = MARGO_REGISTER(*mid, "ff_rpc", ff_rpc_in_t, void, ff_rpc);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, this, NULL);

        id = MARGO_REGISTER(*mid, "ff_rpc_shutdown", void, void, ff_rpc_shutdown);
        margo_registered_disable_response(*mid, id, HG_TRUE);
    }


    void init_mid(char* address, margo_instance_id* mid) {
        margo_init_info args = {
            .json_config   = NULL,      /* const char*          */
            .progress_pool = pool_e1,   /* ABT_pool             */
            .rpc_pool      = pool_e1,   /* ABT_pool             */
            .hg_class      = NULL,      /* hg_class_t*          */
            .hg_context    = NULL,      /* hg_context_t*        */
            .hg_init_info  = NULL       /* struct hg_init_info* */
        };

        *mid = margo_init_ext(address, MARGO_SERVER_MODE, &args);
        // TODO: error handling code here

        margo_set_log_level(*mid, MARGO_LOG_TRACE);
    }

    float* svc(float * task) {
        std::vector<ABT_thread*>* threads = new std::vector<ABT_thread*>();
        
        for (auto &&mid : *mids)
        {
            ABT_thread* aux = new ABT_thread();
            ABT_thread_create(pool_e1, wait_fin, mid, NULL, aux);
            (*threads).push_back(aux);
        }

        // TODO: this should be a single call to finalize each ES and free
        //       every pool. Most likely to become a for loop over a vector
        finalize_xstream_cb(xstream_e1);
        // finalize_xstream_cb(xstream_e2);
        ABT_pool_free(&pool_e1);
        // ABT_pool_free(&pool_e2);
        return EOS;
    }

    // TODO: add cleanup code in destructor

};

// Margo communicator node (client)
// TODO: define this as a templated class
class senderStage: public ff_node_t<float> {

private:
    char*                   addr;
    margo_instance_id       mid;
    hg_addr_t               svr_addr;
    hg_id_t                 ff_rpc_id, ff_shutdown_id;
    ABT_pool                pool_e1;
    ABT_xstream             xstream_e1;

public:
    senderStage(char* addr) : addr{addr}, svr_addr{HG_ADDR_NULL} {
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC, ABT_FALSE, &pool_e1);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1, ABT_SCHED_CONFIG_NULL, &xstream_e1);

        init_mid(addr);

        register_rpcs();
    }

    float* svc(float * task) {
        auto &t = *task; 
        ff_rpc_in_t in;
        hg_handle_t h;

        in.task = new float(*task);
        delete task;

        std::cout << "Sending out: " << *in.task << "\n";
        margo_create(mid, svr_addr, ff_rpc_id, &h);
        margo_forward(h, &in);
        margo_destroy(h);

        delete in.task;
        return GO_ON;
    }

    void svc_end() {
        std::cout << "Finalizing...\n";
        
        hg_handle_t h;
        margo_create(mid, svr_addr, ff_shutdown_id, &h);
        margo_forward(h, NULL);
        margo_destroy(h);
        
        margo_finalize(mid);
        finalize_xstream_cb(xstream_e1);
        ABT_pool_free(&pool_e1);
    }

    void register_rpcs() {
        ff_rpc_id = MARGO_REGISTER(mid, "ff_rpc", ff_rpc_in_t, void, NULL);
        margo_registered_disable_response(mid, ff_rpc_id, HG_TRUE);
        margo_addr_lookup(mid, addr, &svr_addr);
        // TODO: add error handling

        ff_shutdown_id = MARGO_REGISTER_PROVIDER(mid, "ff_rpc_shutdown", void, void, NULL, MARGO_DEFAULT_PROVIDER_ID, ABT_POOL_NULL);
        margo_registered_disable_response(mid, ff_shutdown_id, HG_TRUE);
    }

    void init_mid(char* address) {
        char*                  proto;
        char*                  colon;

        /* initialize Mercury using the transport portion of the destination
        * address (i.e., the part before the first : character if present)
        */
        proto = strdup(address);
        assert(proto);
        colon = strchr(proto, ':');
        if (colon) *colon = '\0';

        margo_init_info args = {
            .json_config   = NULL,      /* const char*          */
            .progress_pool = pool_e1,   /* ABT_pool             */
            .rpc_pool      = pool_e1,   /* ABT_pool             */
            .hg_class      = NULL,      /* hg_class_t*          */
            .hg_context    = NULL,      /* hg_context_t*        */
            .hg_init_info  = NULL       /* struct hg_init_info* */
        };

        mid = margo_init_ext(proto, MARGO_CLIENT_MODE, &args);
        if (mid == MARGO_INSTANCE_NULL) {
            fprintf(stderr, "Error: margo_init_ext()\n");
            // FIXME: We must have a way to manage wrong allocation of mid class
            // return -1;
        }
        margo_set_log_level(mid, MARGO_LOG_TRACE);
        free(proto);

    }
};


/* ----- RPC FUNCTIONS DEFINITION ----- */

void ff_rpc(hg_handle_t handle)
{
    hg_return_t           hret;
    ff_rpc_in_t           in;
    const struct hg_info* hgi;
    margo_instance_id mid;

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);
    /* get handle info and margo instance */
    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);
    assert(mid != MARGO_INSTANCE_NULL);

    const struct hg_info* info = margo_get_info(handle);

    receiverStage* receiver = (receiverStage*)margo_registered_data(mid, info->id);

    receiver->ff_send_out(new float(*in.task));

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc)


void ff_rpc_shutdown(hg_handle_t handle)
{
    hg_return_t       hret;
    margo_instance_id mid;

    printf("Got RPC request to shutdown\n");

    mid = margo_hg_handle_get_instance(handle);
    assert(mid != MARGO_INSTANCE_NULL);

    margo_destroy(handle);
    margo_finalize(mid);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown)

/* ----- RPC FUNCTIONS DEFINITION ----- */