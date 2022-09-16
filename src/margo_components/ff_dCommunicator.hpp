#ifndef FF_DCOMM
#define FF_DCOMM

#include <ff/ff.hpp>
#include <ff/dff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_batchbuffer.hpp>
#include <margo.h>

#include "ff_dCommI.hpp"
#include "../ff_dAreceiverComp.hpp"
#include "ff_drpc_types.h"
#include "ff_margo_utils.hpp"

class ff_dCommRPC: public ff_dCommunicatorRPC {

protected:
    void init_ABT() {
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC,
            ABT_FALSE, &pool_e1);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1,
            ABT_SCHED_CONFIG_NULL, &xstream_e1);
    }


    void init_mid(const char* address, margo_instance_id* mid) {
        na_init_info na_info = NA_INIT_INFO_INITIALIZER;
        na_info.progress_mode = busy ? NA_NO_BLOCK : 0;

        hg_init_info info = {
            .na_init_info = na_info
        };

        margo_init_info args = {
            .progress_pool = pool_e1,   /* ABT_pool             */
            .rpc_pool      = pool_e1,   /* ABT_pool             */
            .hg_init_info  = &info       /* struct hg_init_info* */
        };

        *mid = margo_init_ext(address, MARGO_SERVER_MODE, &args);
        assert(*mid != MARGO_INSTANCE_NULL);
        // margo_set_log_level(*mid, MARGO_LOG_TRACE);

        // Check if the listening address is the requested one
        char addr_self_string[128];
        get_self_addr(mid, addr_self_string);
        // fprintf(stderr, "# accepting RPCs on address \"%s\"\n",
        //     addr_self_string);
    }


    void register_rpcs(margo_instance_id* mid, void* data) {
        hg_id_t id = MARGO_REGISTER(*mid, "ff_rpc", ff_rpc_in_t, void, ff_rpc_comm);
        // NOTE: we actually want a response in the non-blocking version
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, data, NULL);

        id = MARGO_REGISTER(*mid, "ff_rpc_shutdown",
                void, void, ff_rpc_shutdown_comm);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, data, NULL);

        if (internal) {
            hg_id_t id = MARGO_REGISTER(*mid, "ff_rpc_internal",
                    ff_rpc_in_t, void, ff_rpc_internal_comm);
            margo_registered_disable_response(*mid, id, HG_TRUE);
            margo_register_data(*mid, id, data, NULL);

            id = MARGO_REGISTER(*mid, "ff_rpc_shutdown_internal",
                    void, void, ff_rpc_shutdown_internal_comm);
            margo_registered_disable_response(*mid, id, HG_TRUE);
            margo_register_data(*mid, id, data, NULL);
        }
    }


public:
    ff_dCommRPC(ff_endpoint handshakeAddr, size_t input_channels,
        std::vector<ff_endpoint_rpc*> endRPC, bool internal=false, bool busy=true):
        ff_dCommunicatorRPC(handshakeAddr, input_channels), endRPC(std::move(endRPC)),
        internal(internal), busy(busy) {}

    virtual void boot_component() {
        init_ABT();
        for (auto &&addr: this->endRPC)
        {
            margo_instance_id* mid = new margo_instance_id();
            init_mid(addr->margo_addr.c_str(), mid);
            mids.push_back(mid);
        }
    }

    virtual void init(ff_monode_t<message_t> *data) {
        for (auto &&mid: mids)
        {
            register_rpcs(mid, data);
        }
    }

    virtual int comm_listen() {
        if(ff_dCommunicatorRPC::comm_listen() == -1)
            return -1;

        std::vector<ABT_thread*> threads;

        for (auto &&mid : mids)
        {
            ABT_thread* aux = new ABT_thread();
            ABT_thread_create(pool_e1, wait_fin, mid, NULL, aux);
            threads.push_back(aux);
        }

        finalize_xstream_cb(xstream_e1);
        ABT_pool_free(&pool_e1);
        
        return 0;
    }

    virtual void finalize() {
        close(this->listen_sck);
        for (auto &&mid : mids)
        {
            margo_finalize(*mid);
        }
    }


protected:
    std::vector<ff_endpoint_rpc*>   endRPC;
    bool internal = false;
    int                             busy;
    std::vector<margo_instance_id*> mids;
    ABT_pool                        pool_e1;
    ABT_xstream                     xstream_e1;
};


class ff_dCommRPCS: public ff_dCommunicatorRPCS {

protected:
    void init_ABT() {
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC,
            ABT_FALSE, &pool_e1);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1,
            ABT_SCHED_CONFIG_NULL, &xstream_e1);
    }


    void init_mid(const char* address, margo_instance_id* mid) {
        na_init_info na_info = NA_INIT_INFO_INITIALIZER;
        na_info.progress_mode = busy ? NA_NO_BLOCK : 0;

        hg_init_info info = {
            .na_init_info = na_info
        };

        margo_init_info args = {
            .progress_pool = pool_e1,   /* ABT_pool             */
            .rpc_pool      = pool_e1,   /* ABT_pool             */
            .hg_init_info  = &info       /* struct hg_init_info* */
        };

        *mid = margo_init_ext(address, MARGO_SERVER_MODE, &args);
        assert(*mid != MARGO_INSTANCE_NULL);
        // margo_set_log_level(*mid, MARGO_LOG_TRACE);

        // Check if the listening address is the requested one
        char addr_self_string[128];
        get_self_addr(mid, addr_self_string);
        // fprintf(stderr, "# accepting RPCs on address \"%s\"\n",
            // addr_self_string);
    }

    void register_rpcs(margo_instance_id* mid) {
        ff_erpc_id = MARGO_REGISTER(*mid, "ff_rpc", ff_rpc_in_t, void, NULL);
        margo_registered_disable_response(*mid, ff_erpc_id, HG_TRUE);

        ff_eshutdown_id = MARGO_REGISTER(*mid, "ff_rpc_shutdown",
                void, void, NULL);
        margo_registered_disable_response(*mid, ff_eshutdown_id, HG_TRUE);

        // Extended to register internal communication RPCs
        if(internal) {
            ff_irpc_id = MARGO_REGISTER(*mid, "ff_rpc_internal",
                    ff_rpc_in_t, void, NULL);
            margo_registered_disable_response(*mid, ff_irpc_id, HG_TRUE);

            ff_ishutdown_id = MARGO_REGISTER(*mid, "ff_rpc_shutdown_internal",
                    void, void, NULL);
            margo_registered_disable_response(*mid, ff_ishutdown_id, HG_TRUE);
        }
    }

    int getNextSck(bool external) {
        int chid = -2;
        
        ChannelType ct = external ? ChannelType::FWD : ChannelType::INT;
        while(chid == -2) {
            if(ct == rr_iterator->first.second) {
                chid = rr_iterator->first.first;
            }
            if (++rr_iterator == dest2Socket.cend()) rr_iterator = dest2Socket.cbegin();
        }
        return chid;
    }

    hg_handle_t shipRPC(ff_endpoint_rpc* endp, hg_id_t& rpc_id) {
        hg_handle_t h;
        hg_addr_t svr_addr;

        const char* proto = endp->protocol.c_str();
        margo_addr_lookup(*proto2Margo[proto], endp->margo_addr.c_str(), &svr_addr);
        assert(svr_addr);

        margo_create(*proto2Margo[proto], svr_addr, rpc_id, &h);

        return h;
    }

    void forwardRequest(message_t* task, hg_id_t rpc_id, ff_endpoint_rpc* endp) {
        ff_rpc_in_t in;
        in.task = new message_t(task->data.getPtr(), task->data.getLen(), true);
        in.task->chid = task->chid;
        in.task->sender = task->sender;

        hg_handle_t h = shipRPC(endp, rpc_id);
        margo_forward(h, &in);
        margo_destroy(h);
        
        delete in.task;
    }

    void forwardEOS(message_t* task, hg_id_t rpc_id, ff_endpoint_rpc* endp) {
        hg_handle_t h = shipRPC(endp, rpc_id);
        margo_forward(h, NULL);
        margo_destroy(h);
    }

public:

    ff_dCommRPCS(std::pair<ChannelType, ff_endpoint> destEndpoint,
        std::vector<ff_endpoint_rpc*> endRPC,
        std::string gName = "", bool internal=false, bool busy = true):
            ff_dCommunicatorRPCS(destEndpoint, gName), busy(busy),
            endRPC(std::move(endRPC)), internal(internal) {}

    ff_dCommRPCS(std::vector<std::pair<ChannelType,ff_endpoint>> destEndpoints,
        std::vector<ff_endpoint_rpc*> endRPC,
        std::string gName = "", bool internal=false, bool busy = true):
            ff_dCommunicatorRPCS(destEndpoints, gName), busy(busy),
            endRPC(std::move(endRPC)), internal(internal) {}

    virtual void boot_component() {
        init_ABT();
        for (auto &&endp: endRPC)
        {
            // We don't care about the address used for this mid, we only need
            // the same protocol used by the endpoint to contact
            char* proto;
            proto = strdup(endp->protocol.c_str());
            
            // We only create a new mid if there is still no margo instance
            // using this protocol
            if(proto2Margo.find(proto) == proto2Margo.end()) {
                margo_instance_id* mid = new margo_instance_id();
                init_mid(proto, mid);
                register_rpcs(mid);
                proto2Margo.insert({proto, mid});
            }

            free(proto);
        }          
    }

    virtual void init() {
        return; 
    }

    virtual int send(message_t* task, bool external) {
        ff_endpoint_rpc* endp;
        hg_id_t rpc_id;
        count++;
        int sck;
        if (task->chid == -1)
            task->chid = getNextSck(external);
            
        if(external) sck = dest2Socket[{task->chid, task->feedback ? ChannelType::FBK : ChannelType::FWD}]; 
        else sck = dest2Socket[{task->chid, ChannelType::INT}];

        rpc_id = external ? ff_erpc_id : ff_irpc_id;
        
        endp = sock2End[sck];
        forwardRequest(task, rpc_id, endp);

        return 0;
    }

    virtual void notify(ssize_t id, bool external) {
        if(external) 
            for (auto& sck : sockets){
                forwardRequest(new message_t(id, -2), ff_erpc_id, sock2End[sck]);
            }
        else {
            // send the EOS to all the internal connections
            for(const auto& sck : internalSockets){
                forwardEOS(NULL, ff_ishutdown_id, sock2End[sck]);
            }
        }
    }

    virtual void finalize() {
        ff_dCommunicatorRPCS::finalize();
        for(auto& sck : sockets){
            forwardEOS(NULL, ff_eshutdown_id, sock2End[sck]);
        }

        for (auto &&mid : proto2Margo)
        {
            margo_finalize(*mid.second);
        }
        finalize_xstream_cb(xstream_e1);
        ABT_pool_free(&pool_e1);
    }

    virtual int handshake(precomputedRT_t* rt) {
        int ret = ff_dCommunicatorRPCS::handshake(rt);
        rr_iterator = dest2Socket.cbegin();

        for (size_t i = 0; i < socks.size(); i++) {
            sock2End.insert({socks[i], this->endRPC[i]});
        }

        return ret;
    }

public:
    // Extension for RPC based communication
    int                                         busy;
    std::vector<ff_endpoint_rpc*>               endRPC;
    bool                                        internal;
    
    std::vector<margo_instance_id*>             mids;
    ABT_pool                                    pool_e1;
    ABT_xstream                                 xstream_e1;
    std::map<std::string, margo_instance_id*>   proto2Margo;
    std::map<int, ff_endpoint_rpc*>             sock2End; 

    hg_id_t                                     ff_erpc_id, ff_eshutdown_id;
    hg_id_t                                     ff_irpc_id, ff_ishutdown_id;

    std::map<std::pair<int, ChannelType>, int>::const_iterator  rr_iterator;
    int count = 0;
};


void ff_rpc_comm(hg_handle_t handle) {
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

    ff_dAreceiver* receiver =
        (ff_dAreceiver*)margo_registered_data(mid, info->id);

    receiver->received++;
    if(in.task->chid == -2)
        receiver->registerLogicalEOS(in.task->sender);
    receiver->forward(in.task, false);

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_comm)


void ff_rpc_internal_comm(hg_handle_t handle) {
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

    ff_dAreceiver* receiver =
        (ff_dAreceiver*)margo_registered_data(mid, info->id);

    receiver->received++;
    if(in.task->chid == -2)
        receiver->registerLogicalEOS(in.task->sender);
    else receiver->forward(in.task, true);

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_internal_comm)


void ff_rpc_shutdown_comm(hg_handle_t handle) {
    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    ff_dAreceiver* receiver =
        (ff_dAreceiver*)margo_registered_data(mid, info->id);
    
    receiver->registerEOS(false);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown_comm);


void ff_rpc_shutdown_internal_comm(hg_handle_t handle) {
    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    ff_dAreceiver* receiver =
        (ff_dAreceiver*)margo_registered_data(mid, info->id);
    
    receiver->registerEOS(true);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown_internal_comm);


#endif //FFDCOMM
