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
#include "dist_rpc_type.h"

using namespace ff;



/*
 * An RPC-based receiver node enabled for both internal and external
 * communications on multiple endpoints.
 */
class ff_dreceiverRPC: public ff_dreceiverH {
protected:
    void init_ABT() {
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC,
            ABT_FALSE, &pool_e1);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1,
            ABT_SCHED_CONFIG_NULL, &xstream_e1);
    }

    void init_mid(const char* address, margo_instance_id* mid) {
        na_init_info na_info;
        na_info.progress_mode = busy ? NA_NO_BLOCK : 0;

        hg_init_info info = {
            .na_init_info = na_info
        };

        margo_init_info args = {
            .progress_pool  = pool_e1,
            .rpc_pool       = pool_e1,
            .hg_init_info   = &info
        };

        *mid = margo_init_ext(address, MARGO_SERVER_MODE, &args);
        assert(*mid != MARGO_INSTANCE_NULL);

        // Check if the listening address is the requested one
        char addr_self_string[128];
        get_self_addr(mid, addr_self_string);
        fprintf(stderr, "# accepting RPCs on address \"%s\"\n",
            addr_self_string);
    }

    void register_rpcs(margo_instance_id* mid) {
        hg_id_t id = MARGO_REGISTER(*mid, "ff_rpc", ff_rpc_in_t, void, ff_rpc);
        // NOTE: we actually want a response in the non-blocking version
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, this, NULL);

        id = MARGO_REGISTER(*mid, "ff_rpc_internal",
                ff_rpc_in_t, void, ff_rpc_internal);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, this, NULL);

        id = MARGO_REGISTER(*mid, "ff_rpc_shutdown",
                void, void, ff_rpc_shutdown);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, this, NULL);

        id = MARGO_REGISTER(*mid, "ff_rpc_shutdown_internal",
                void, void, ff_rpc_shutdown_internal);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, this, NULL);
    }

    void registerEOS(bool internal) {
        neos++;
        // NOTE: the internalConn variable can be saved once and for all at the end
        //      of the handshake process. This will not change once we have received
        //      all connection requests
        size_t internalConn = std::count_if(std::begin(isInternalConnection),
                                        std::end  (isInternalConnection),
                                        [](std::pair<int, bool> const &p) {return p.second;});

        
        if(!internal) {
            if (++externalNEos == (isInternalConnection.size()-internalConn))
                for(size_t i = 0; i < get_num_outchannels()-1; i++) ff_send_out_to(this->EOS, i);
        } else {
            if (++internalNEos == internalConn)
                ff_send_out_to(this->EOS, get_num_outchannels()-1);
        }

        if(neos == input_channels)
            for (auto &&mid : mids)
            {
                margo_finalize(*mid);
            }
            
    }


public:
    //FIXME: in sender you used ff_endpoint_rpc*, here you are using copy not pointer
    // Multi-endpoint extension
    ff_dreceiverRPC(ff_endpoint handshakeAddr,
        std::vector<ff_endpoint_rpc> acceptAddrs, size_t input_channels,
        std::map<int, int> routingTable = {{0,0}},
        std::vector<int> internalRoutingTable = {0},
        std::set<std::string> internalGroups = {}, int coreid=-1, int busy=0):
            ff_dreceiverH(handshakeAddr, input_channels, routingTable,
                internalRoutingTable, internalGroups, coreid),
            acceptAddrs(std::move(acceptAddrs)), busy(busy) {

        init_ABT();
        for (auto &&addr: this->acceptAddrs)
        {
            //TODO: add checking for existing mid with same protocol
            margo_instance_id* mid = new margo_instance_id();
            init_mid(addr.margo_addr.c_str(), mid);
            register_rpcs(mid);
            mids.push_back(mid);
        }
    
    }

    message_t* svc(message_t* task) {
        fd_set set, tmpset;
        // intialize both sets (master, temp)
        FD_ZERO(&set);
        FD_ZERO(&tmpset);

        // add the listen socket to the master set
        FD_SET(this->listen_sck, &set);

        // hold the greater descriptor
        int fdmax = this->listen_sck; 

        // We only need to receive routing tables once per input channel
        while(handshakes < input_channels){
            // copy the master set to the temporary
            tmpset = set;

            switch(select(fdmax+1, &tmpset, NULL, NULL, NULL)){
                case -1: error("Error on selecting socket\n"); return EOS;
                case  0: continue;
            }

            // iterate over the file descriptor to see which one is active
            int fixed_last = (this->last_receive_fd + 1) % (fdmax +1);
            for(int i=0; i <= fdmax; i++){
                int actualFD = (fixed_last + i) % (fdmax +1);
	            if (FD_ISSET(actualFD, &tmpset)){
                    if (actualFD == this->listen_sck) {
                        int connfd = accept(this->listen_sck, (struct sockaddr*)NULL ,NULL);
                        if (connfd == -1){
                            error("Error accepting client\n");
                        } else {
                            if(connfd > fdmax) fdmax = connfd;

                            this->handshakeHandler(connfd);
                            handshakes++;
                            close(connfd);
                        }
                        continue;
                    }
                }
            }
        }

        std::vector<ABT_thread*> threads;

        for (auto &&mid : mids)
        {
            ABT_thread* aux = new ABT_thread();
            ABT_thread_create(pool_e1, wait_fin, mid, NULL, aux);
            threads.push_back(aux);
        }

        finalize_xstream_cb(xstream_e1);
        ABT_pool_free(&pool_e1);
        return this->EOS;
        
    }

    // Necessary to access internal fields in the RPC callbacks
    friend void ff_rpc(hg_handle_t handle);
    friend void ff_rpc_shutdown(hg_handle_t handle);
    friend void ff_rpc_shutdown_internal(hg_handle_t handle);

protected:
    std::vector<ff_endpoint_rpc>    acceptAddrs;
    std::vector<margo_instance_id*> mids;
    ABT_pool                        pool_e1;
    ABT_xstream                     xstream_e1;
    int                             busy;
    int                             handshakes = 0;
};


void ff_rpc(hg_handle_t handle) {
    hg_return_t             hret;
    ff_rpc_in_t             in;
    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    ff_dreceiverRPC* receiver =
        (ff_dreceiverRPC*)margo_registered_data(mid, info->id);
        
    receiver->ff_send_out_to(in.task, receiver->routingTable[in.task->chid]);

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc)


void ff_rpc_internal(hg_handle_t handle) {
    hg_return_t             hret;
    ff_rpc_in_t             in;
    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    ff_dreceiverRPC* receiver =
        (ff_dreceiverRPC*)margo_registered_data(mid, info->id);

    receiver->ff_send_out_to(in.task, receiver->get_num_outchannels()-1);

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_internal)


void ff_rpc_shutdown(hg_handle_t handle) {
    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    ff_dreceiverRPC* receiver =
        (ff_dreceiverRPC*)margo_registered_data(mid, info->id);
    
    receiver->registerEOS(false);

    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown);


void ff_rpc_shutdown_internal(hg_handle_t handle) {

    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    ff_dreceiverRPC* receiver =
        (ff_dreceiverRPC*)margo_registered_data(mid, info->id);

    receiver->registerEOS(true);

    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown_internal);