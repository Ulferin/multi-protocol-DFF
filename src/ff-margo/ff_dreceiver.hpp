/**
 * @file ff_comm.hpp
 * @author Federico Finocchio
 * @brief Implementation of two basics remotely connected ff_node_t classes.
 * The nodes are purposefully implemented to be plugged in existing FastFlow
 * building blocks and communicate between each other's, since they will happen
 * to be paired at the extremes of two distributed groups. Thus, both nodes
 * share the same set of RPC functions which allows to forward/receive an
 * element of a stream through the network.
 * 
 * 
 * Two types of remotely connected nodes are provided:
 *   - receiverStage: a listening node for remote RPC functions. It can listen
 *          on multiple endpoints and forwards every input element
 *          received to the next FastFlow node. Generates an EOS upon shutdown.
 *   - senderStage: a remote forwarder node which ships stream elements through
 *          the registered RPC functions. Issue a shutdown request upon EOS.
 * 
 * Both nodes register two RPCs, ff_rpc used to pass stream elements between
 * remote ends, ff_rpc_shutdown used to signal to the remotely connected group
 * that an EOS has been received.
 * 
 * 
 * @version 0.1
 * @date 2022-03-21
 * 
 * 
 */

#include <iostream>
#include <vector>

#include <ff/ff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_dgroups.hpp>

#include <margo.h>
#include <abt.h>

#include "my-rpc.h"
#include "utils.hpp"

using namespace ff;



class ff_dreceiver_rpc: public ff_monode_t<message_t> {

private:
    hg_return_t         hret;
    std::vector<margo_instance_id*>* mids;
    
    hg_addr_t           addr_self;
    ABT_pool            pool_e1;
    ABT_xstream         xstream_e1;
    int                 busy;


    void register_rpcs(margo_instance_id* mid) {
        hg_id_t id = MARGO_REGISTER(*mid, "ff_rpc", ff_rpc_in_t, void, ff_rpc);
        // TODO: we actually want a response in the non-blocking version
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, this, NULL);

        id = MARGO_REGISTER(*mid, "ff_rpc_shutdown", void, void, ff_rpc_shutdown);
        margo_registered_disable_response(*mid, id, HG_TRUE);
    }


    void init_mid(char* address, margo_instance_id* mid) {
        na_init_info na_info;
        na_info.progress_mode = busy ? NA_NO_BLOCK : 0;
        
        hg_init_info info = {
            .na_init_info = na_info
        };

        margo_init_info args = {
            .json_config   = NULL,      /* const char*          */
            .progress_pool = pool_e1,   /* ABT_pool             */
            .rpc_pool      = pool_e1,   /* ABT_pool             */
            .hg_class      = NULL,      /* hg_class_t*          */
            .hg_context    = NULL,      /* hg_context_t*        */
            .hg_init_info  = &info       /* struct hg_init_info* */
        };

        *mid = margo_init_ext(address, MARGO_SERVER_MODE, &args);
        // FIXME: error handling code here
    }


public:
    ff_dreceiver_rpc(std::vector<char*> addresses, int busy=0) : 
                busy{busy} {

        mids = new std::vector<margo_instance_id*>();
        
        // Set up pool and xstreams to manage RPC requests
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC,
                ABT_FALSE, &pool_e1);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1,
                ABT_SCHED_CONFIG_NULL, &xstream_e1);

        // Init margo instances and register the set of RPCs
        for (auto &&addr : addresses)
        {
            margo_instance_id* mid = new margo_instance_id();
            init_mid(addr, mid);
            register_rpcs(mid);
            (*mids).push_back(mid);

            // Check if the listening address is the requested one
            char addr_self_string[128];
            get_self_addr(mid, addr_self_string);
            fprintf(stderr, "# accepting RPCs on address \"%s\"\n",
                addr_self_string);
        }

    }


    message_t* svc(message_t * task) {
        std::vector<ABT_thread*> threads;
        
        // Wait for a call to margo_finalize
        for (auto &&mid : *mids)
        {
            ABT_thread* aux = new ABT_thread();
            ABT_thread_create(pool_e1, wait_fin, mid, NULL, aux);
            threads.push_back(aux);
        }

        // TODO: this should be a single call to finalize each ES and free
        //       every pool. Most likely to become a for loop over a vector
        finalize_xstream_cb(xstream_e1);
        ABT_pool_free(&pool_e1);
        return this->EOS;
    }

    // TODO: add cleanup code in destructor

};


/* ----- MARGO RPCs definition ----- */

void ff_rpc(hg_handle_t handle)
{
    hg_return_t           hret;
    ff_rpc_in_t           in;
    const struct hg_info* info;
    margo_instance_id mid;


    // Get handle info and margo instance to retrieve input and registered data
    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    // Get received input
    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    // Retrieve registered receiver object and forward input to next stage
    ff_dreceiver_rpc* receiver =
            (ff_dreceiver_rpc*)margo_registered_data(mid, info->id);
    std::cout << "[receiver]received: " << *(in.task) << "\n";
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

/* ----- MARGO RPCs definition ----- */