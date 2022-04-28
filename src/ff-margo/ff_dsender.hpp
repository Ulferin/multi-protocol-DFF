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

// TODO: update file description

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


// TODO: define this as a templated class
class ff_dsender_rpc: public ff_minode_t<message_t> {

private:
    char*                   addr;
    margo_instance_id       mid;
    hg_addr_t               svr_addr;
    hg_id_t                 ff_rpc_id, ff_shutdown_id;
    ABT_pool                pool_e1;
    ABT_xstream             xstream_e1;
    int                     busy;


    void register_rpcs() {
        ff_rpc_id = MARGO_REGISTER(mid, "ff_rpc", ff_rpc_in_t, void, NULL);
        margo_registered_disable_response(mid, ff_rpc_id, HG_TRUE);
        margo_addr_lookup(mid, addr, &svr_addr);
        // TODO: add error handling

        ff_shutdown_id = MARGO_REGISTER(mid, "ff_rpc_shutdown",
                void, void, NULL);
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

        // NOTE: we are listening on a "client" node. Necessary in order to
        //       avoid UCX error on printing address
        mid = margo_init_ext(proto, MARGO_SERVER_MODE, &args);
        if (mid == MARGO_INSTANCE_NULL) {
            fprintf(stderr, "Error: margo_init_ext()\n");
            // FIXME: We must have a way to manage wrong allocation of mid class
            // return -1;
        }
        // margo_set_log_level(mid, MARGO_LOG_TRACE);
        free(proto);

    }

public:
    // FIXME: modify this in order to use move semantic to transfer ownerhsip of
    //       address string.
    ff_dsender_rpc(char* addr, int busy=0) : addr{addr},
                        busy{busy}, svr_addr{HG_ADDR_NULL} {
        
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC, ABT_FALSE,
                &pool_e1);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1,
                ABT_SCHED_CONFIG_NULL, &xstream_e1);

        init_mid(addr);

        register_rpcs();
    }


    message_t* svc(message_t* task) {
        auto &t = *task; 
        ff_rpc_in_t in;
        hg_handle_t h;

        in.task = new message_t(task->data.getPtr(), task->data.getLen(), true);
        in.task->chid = task->chid;
        in.task->sender = task->sender;
        delete task;

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
};