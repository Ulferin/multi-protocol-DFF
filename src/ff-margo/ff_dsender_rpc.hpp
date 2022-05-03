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

//TODO: update file description
//TODO: create two classes to distinguish between external/internal case. This
//      is necessary to deal with messages coming from RBox or others, since the
//      RBox node will not be present in external case

#include <iostream>
#include <vector>

#include <ff/ff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_dgroups.hpp>

#include <margo.h>
#include <abt.h>

#include "dist_rpc_type.h"

using namespace ff;


class ff_dsenderRPCH: public ff_dsenderH {

protected:
    void init_ABT() {
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC,
            ABT_FALSE, &pool_e1);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1,
            ABT_SCHED_CONFIG_NULL, &xstream_e1);
    }

    void init_mid(const char* proto, margo_instance_id* mid) {
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

        *mid = margo_init_ext(proto, MARGO_CLIENT_MODE, &args);
        assert(*mid != MARGO_INSTANCE_NULL);
        printf("initializing: %s\n", proto);
    }

    void register_rpcs(margo_instance_id* mid) {
        ff_erpc_id = MARGO_REGISTER(*mid, "ff_rpc", ff_rpc_in_t, void, NULL);
        // NOTE: we actually want a response in the non-blocking version
        margo_registered_disable_response(*mid, ff_erpc_id, HG_TRUE);

        ff_irpc_id = MARGO_REGISTER(*mid, "ff_rpc_internal",
                ff_rpc_in_t, void, NULL);
        margo_registered_disable_response(*mid, ff_irpc_id, HG_TRUE);

        ff_eshutdown_id = MARGO_REGISTER(*mid, "ff_rpc_shutdown",
                void, void, NULL);
        margo_registered_disable_response(*mid, ff_eshutdown_id, HG_TRUE);

        ff_ishutdown_id = MARGO_REGISTER(*mid, "ff_rpc_shutdown_internal",
                void, void, NULL);
        margo_registered_disable_response(*mid, ff_ishutdown_id, HG_TRUE);
    }

    void startup() {
        init_ABT();
        for (auto &&addr: endRPC)
        {
            // We don't care about the address used for this mid, we only need
            // the same protocol used by the endpoint to contact
            std::string proto((*addr).margo_addr.c_str());
            assert(proto.c_str());
            size_t colon = proto.find_first_of(':');
            proto.resize(colon);
            printf("[startup]init: margo=%s - proto=%s\n", addr->margo_addr.c_str(), proto.c_str());
            // We only create a new mid if there is still no margo instance
            // using this protocol
            if(proto2Margo.find(proto) == proto2Margo.end()) {
                margo_instance_id* mid = new margo_instance_id();
                init_mid(proto.c_str(), mid);
                register_rpcs(mid);
                proto2Margo.insert({proto.c_str(), mid});
            }
        }
    }

    void forwardRequest(message_t* task, hg_id_t rpc_id, ff_endpoint_rpc* endp) {
        hg_handle_t h;
        hg_addr_t svr_addr;
        
        ff_rpc_in_t in;
        in.task = new message_t(task->data.getPtr(), task->data.getLen(), true);
        in.task->chid = task->chid;
        in.task->sender = task->sender;
        delete task;

        std::string proto((*endp).margo_addr.c_str());
        assert(proto.c_str());
        size_t colon = proto.find_first_of(':');
        proto.resize(colon);

        margo_addr_lookup(*proto2Margo[proto.c_str()], endp->margo_addr.c_str(), &svr_addr);

        margo_create(*proto2Margo[proto.c_str()], svr_addr, rpc_id, &h);
        margo_forward(h, &in);
        margo_destroy(h);
        delete in.task;
    }


public:
    ff_dsenderRPCH(ff_endpoint e, std::vector<ff_endpoint_rpc*> endRPC = {},
        std::string gName = "", std::set<std::string> internalGroups = {},
        int coreid = -1, int busy = 1):
            ff_dsenderH(e, gName, internalGroups, coreid),
            endRPC(std::move(endRPC)), busy(busy) {}
    
    ff_dsenderRPCH(std::vector<ff_endpoint> dest_endpoints_,
        std::vector<ff_endpoint_rpc*> endRPC = {},
        std::string gName = "", std::set<std::string> internalGroups = {},
        int coreid=-1, int busy = 1):
            ff_dsenderH(dest_endpoints_, gName, internalGroups, coreid),
            endRPC(std::move(endRPC)), busy(busy) {}


    int svc_init() {
        startup();
        //FIXME: this can may be rewritten by leveraging the original svc_init
        //      of receiverH class

        for (int i = 0; i < this->dest_endpoints.size(); i++)
        {
            int sck = tryConnect(this->dest_endpoints[i]);
            if (sck <= 0) {
                error("Error on connecting!\n");
                return -1;
            }

            bool isInternal = internalGroupNames.contains(this->dest_endpoints[i].groupName);
            handshakeHandler(sck, isInternal);
            if (isInternal) internalSockets.push_back(sck);
            else sockets.push_back(sck);
            //NOTE: probably this can be substituted by creating the handle for
            //      the RPC as it is done exactly before the forwarding process
            //      the saved handle can then be used in the communications
            //      involving this "socket" descriptor
            sock2End.insert({sck, endRPC[i]});

            //NOTE: if we close the socket this class won't work in every context
            //      as the class it extends, violating the substitution principle
            close(sck);
        }

        rr_iterator = internalDest2Socket.cbegin();
        return 0;
    }

    message_t *svc(message_t* task) {
        ff_endpoint_rpc* endp;
        hg_id_t rpc_id;
        
        // Conditionally retrieve endpoint information and RPC id based on
        // internal/external chid.
        if (this->get_channel_id() == (ssize_t)(this->get_num_inchannels() - 1)){
            // pick destination from the list of internal connections!
            if (task->chid == -1){ // roundrobin over the destinations
                task->chid = rr_iterator->first;
                if (++rr_iterator == internalDest2Socket.cend()) rr_iterator = internalDest2Socket.cbegin();
            }

            rpc_id = ff_irpc_id;
            int sck = internalDest2Socket[task->chid];
            endp = sock2End[sck];
        }
        else {
            if (task->chid == -1){ // roundrobin over the destinations
                task->chid = next_rr_destination;
                next_rr_destination = (next_rr_destination + 1) % dest2Socket.size();
            }
            rpc_id = ff_erpc_id;
            int sck = dest2Socket[task->chid];
            endp = sock2End[sck];
        }

        forwardRequest(task, rpc_id, endp);
        return this->GO_ON;

    }

    void eosnotify(ssize_t id) {
        message_t E_O_S(0,0);
        hg_id_t rpc_id;
        ff_endpoint_rpc* endp;
        if (id == (ssize_t)(this->get_num_inchannels() - 1)){
            // send the EOS to all the internal connections
            for(const auto& sck : internalSockets) {
                rpc_id = ff_ishutdown_id;
                endp = sock2End[sck];
                forwardRequest(&E_O_S, rpc_id, endp);
            }           
        }
        if (++neos >= this->get_num_inchannels()){
            for(const auto& sck : sockets) {
                rpc_id = ff_eshutdown_id;
                endp = sock2End[sck];
                forwardRequest(&E_O_S, rpc_id, endp);
            }
        }
     }


protected:
    std::vector<ff_endpoint_rpc*>               endRPC;
    std::map<int, ff_endpoint_rpc*>             sock2End; //FIXME: this should really be a "std::pair" to keep both ff_endpoint_rpc for info about the connection and hg_addr_t for handle for the provided address.
    std::map<std::string, margo_instance_id*>   proto2Margo;

    hg_id_t                                     ff_erpc_id, ff_irpc_id, ff_eshutdown_id, ff_ishutdown_id;
    ABT_pool                                    pool_e1;
    ABT_xstream                                 xstream_e1;
    int                                         busy;

};