#ifndef FF_DCOMM
#define FF_DCOMM

#include <ff/ff.hpp>
#include <ff/dff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_batchbuffer.hpp>
#include <margo.h>

#include "ff_dCommI.hpp"
#include "ff_dAreceiver.hpp"
#include "ff_drpc_types.h"
#include "ff_margo_utils.hpp"

class ff_dCommRPC: public ff_dCommunicator {

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
    ff_dCommRPC(ff_endpoint handshakeAddr, bool internal, bool busy,
        std::vector<ff_endpoint_rpc*> endRPC,
        std::vector<int> internalDestinations = {0},
        std::map<int, int> routingTable = {{0,0}},
        std::set<std::string> internalGroupsNames = {}):
        ff_dCommunicator(handshakeAddr, internal, internalDestinations,
            routingTable, internalGroupsNames), busy(busy),
        endRPC(std::move(endRPC)) {}

    virtual void init(ff_monode_t<message_t>* data, int input_channels) {
        //FIXME: rimuovere input channels da qui, abbiamo già messo nel costruttore
        this->input_channels = input_channels;
        init_ABT();
        for (auto &&addr: this->endRPC)
        {
            margo_instance_id* mid = new margo_instance_id();
            init_mid(addr->margo_addr.c_str(), mid);
            register_rpcs(mid, data);
            mids.push_back(mid);
        }
    }

    virtual int comm_listen() {
        if(ff_dCommunicator::comm_listen() == -1)
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
    int                             busy;
    std::vector<ff_endpoint_rpc*>   endRPC;
    std::vector<margo_instance_id*> mids;
    ABT_pool                        pool_e1;
    ABT_xstream                     xstream_e1;

};


class ff_dCommRPCS: public ff_dCommunicatorS {

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
    ff_dCommRPCS(ff_endpoint dest_endpoint, std::vector<ff_endpoint_rpc*> endRPC,
        std::string gName = "", std::set<std::string> internalGroups = {},
        bool internal = false, bool busy = true):
        ff_dCommunicatorS(dest_endpoint, gName, internalGroups),
        internal(internal), busy(busy), endRPC(std::move(endRPC)) {}

    ff_dCommRPCS(std::vector<ff_endpoint> dest_endpoints_,
        std::vector<ff_endpoint_rpc*> endRPC,
        std::string gName = "", std::set<std::string> internalGroups = {},
        bool internal = false, bool busy = true):
        ff_dCommunicatorS(dest_endpoints_, gName, internalGroups),
        internal(internal), busy(busy), endRPC(std::move(endRPC)) {}


    virtual void init() {
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

    virtual void send(message_t* task, bool ext) {
        ff_endpoint_rpc* endp;
        hg_id_t rpc_id;
        count++;
        int sck = ext ? dest2Socket[task->chid] : internalDest2Socket[task->chid];

        rpc_id = ext ? ff_erpc_id : ff_irpc_id;
        if(task->data.getLen() == 0) {
            printf("[SENDER] Forwarding EOS but I only send %d tasks\n", count);
            rpc_id = ext ? ff_eshutdown_id : ff_ishutdown_id;

            if(ext)
                for(const auto& sck : sockets) {
                    endp = sock2End[sck];
                    forwardRequest(task, rpc_id, endp);             
                } 
            else
                for(const auto& sck : internalSockets) {
                    endp = sock2End[sck];
                    forwardRequest(task, rpc_id, endp);             
                }
            
            return;
        }
        endp = sock2End[sck];

        forwardRequest(task, rpc_id, endp);
    }

    virtual void finalize() {
        ff_dCommunicatorS::finalize();

        for (auto &&mid : proto2Margo)
        {
            margo_finalize(*mid.second);
        }
        finalize_xstream_cb(xstream_e1);
        ABT_pool_free(&pool_e1);
    }

    virtual int handshake() {
        int ret = ff_dCommunicatorS::handshake();
        
        for (size_t i = 0; i < socks.size(); i++)
        {
            sock2End.insert({socks[i], this->endRPC[i]});
        }

        return ret;
    }

public:
    // Extension for RPC based communication
    bool                                        internal;
    int                                         busy;
    std::vector<ff_endpoint_rpc*>               endRPC;
    
    std::vector<margo_instance_id*>             mids;
    ABT_pool                                    pool_e1;
    ABT_xstream                                 xstream_e1;
    std::map<std::string, margo_instance_id*>   proto2Margo;
    std::map<int, ff_endpoint_rpc*>             sock2End; 

    hg_id_t                                     ff_erpc_id, ff_eshutdown_id;
    hg_id_t                                     ff_irpc_id, ff_ishutdown_id;

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
    
    //NOTE: this may cause problem while using ucx
    // hg_size_t size = 128;
    // char addr_string[128];
    // margo_addr_to_string(mid, addr_string, &size, info->addr);
    // printf("Received from: %s\n", addr_string);

    ff_dAreceiver* receiver =
        (ff_dAreceiver*)margo_registered_data(mid, info->id);

    receiver->received++;
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
    receiver->forward(in.task, true);

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