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
#include <sstream>
#include <vector>

#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <arpa/inet.h>

#include <ff/ff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_drpc_types.h>
#include <ff/distributed/ff_dgroups.hpp>
#include <ff/distributed/ff_dutils.hpp>
#include <cereal/cereal.hpp>
#include <cereal/archives/portable_binary.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/types/polymorphic.hpp>

#include <margo.h>
#include <abt.h>

// #include "my-rpc.h"

using namespace ff;

class ff_dreceiverRPC: public ff_monode_t<message_t> {
protected:

    void init_ABT() {
        #ifdef INIT_CUSTOM
        margo_set_environment(NULL);
        ABT_init(0, NULL);
        #endif
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC,
            ABT_FALSE, &pool_e1);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1,
            ABT_SCHED_CONFIG_NULL, &xstream_e1);
    }

    void init_mid(const char* address, margo_instance_id* mid) {
        na_init_info na_info;
        na_info.progress_mode = busy ? NA_NO_BLOCK : 0;
        na_info.max_contexts = 1;

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

        id = MARGO_REGISTER(*mid, "ff_rpc_shutdown",
                void, void, ff_rpc_shutdown);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, this, NULL);
    }

    static int sendRoutingTable(const int sck, const std::vector<int>& dest){
        dataBuffer buff; std::ostream oss(&buff);
		cereal::PortableBinaryOutputArchive oarchive(oss);
		oarchive << dest;

        size_t sz = htobe64(buff.getLen());
        struct iovec iov[2];
        iov[0].iov_base = &sz;
        iov[0].iov_len = sizeof(sz);
        iov[1].iov_base = buff.getPtr();
        iov[1].iov_len = buff.getLen();

        if (writevn(sck, iov, 2) < 0){
            error("Error writing on socket the routing Table\n");
            return -1;
        }

        return 0;
    }

    virtual int handshakeHandler(const int sck){
        // ricevo l'handshake e mi salvo che tipo di connessione è
        size_t size;
        struct iovec iov; iov.iov_base = &size; iov.iov_len = sizeof(size);
        switch (readvn(sck, &iov, 1)) {
           case -1: error("Error reading from socket\n"); // fatal error
           case  0: return -1; // connection close
        }

        size = be64toh(size);

        char groupName[size];
        if (readn(sck, groupName, size) < 0){
            error("Error reading from socket groupName\n"); return -1;
        }
        std::vector<int> reachableDestinations;
        for(const auto& [key, value] : this->routingTable) reachableDestinations.push_back(key);

        return this->sendRoutingTable(sck, reachableDestinations);
    }

    virtual void forward(message_t* task, int){
        ff_send_out_to(task, this->routingTable[task->chid]); // assume the routing table is consistent WARNING!!!
    }

    void registerEOS(bool) {
        neos++;    

        if(neos == input_channels)
            for (auto &&mid : mids)
            {
                margo_finalize(*mid);
            }        
    }

    // For TCP-based connections
    virtual int handleRequest(int sck){
   		int sender;
		int chid;
        size_t sz;
        struct iovec iov[3];
        iov[0].iov_base = &sender;
        iov[0].iov_len = sizeof(sender);
        iov[1].iov_base = &chid;
        iov[1].iov_len = sizeof(chid);
        iov[2].iov_base = &sz;
        iov[2].iov_len = sizeof(sz);

        switch (readvn(sck, iov, 3)) {
           case -1: error("Error reading from socket\n"); // fatal error
           case  0: return -1; // connection close
        }

        // convert values to host byte order
        sender = ntohl(sender);
        chid   = ntohl(chid);
        sz     = be64toh(sz);

        if (sz > 0){
            char* buff = new char [sz];
			assert(buff);
            if(readn(sck, buff, sz) < 0){
                error("Error reading from socket\n");
                delete [] buff;
                return -1;
            }
			message_t* out = new message_t(buff, sz, true);
			assert(out);
			out->sender = sender;
			out->chid   = chid;

            this->forward(out, sck);
            return 0;
        }


        registerEOS(sck);

        return -1;
    }

public:
    ff_dreceiverRPC(ff_endpoint handshakeAddr,
        std::vector<ff_endpoint_rpc*> endRPC, size_t input_channels,
        std::map<int, int> routingTable = {std::make_pair(0,0)},
        int coreid = -1, int busy = 0):
            handshakeAddr(handshakeAddr), endRPC(std::move(endRPC)),
            input_channels(input_channels), routingTable(routingTable),
            coreid(coreid) {
        
        init_ABT();
        for (auto &&addr: this->endRPC)
        {
            margo_instance_id* mid = new margo_instance_id();
            init_mid(addr->margo_addr.c_str(), mid);
            register_rpcs(mid);
            mids.push_back(mid);
        }
    }

    int svc_init() {
  		if (coreid!=-1)
			ff_mapThreadToCpu(coreid);

        #ifdef LOCAL
            if ((listen_sck=socket(AF_LOCAL, SOCK_STREAM, 0)) < 0){
                error("Error creating the socket\n");
                return -1;
            }
            
            struct sockaddr_un serv_addr;
            memset(&serv_addr, '0', sizeof(serv_addr));
            serv_addr.sun_family = AF_LOCAL;
            strncpy(serv_addr.sun_path, acceptAddr.address.c_str(), acceptAddr.address.size()+1);
        #endif

        #ifdef REMOTE
            if ((listen_sck=socket(AF_INET, SOCK_STREAM, 0)) < 0){
                error("Error creating the socket\n");
                return -1;
            }

            int enable = 1;
            // enable the reuse of the address
            if (setsockopt(listen_sck, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
                error("setsockopt(SO_REUSEADDR) failed\n");

            struct sockaddr_in serv_addr;
            serv_addr.sin_family = AF_INET; 
            serv_addr.sin_addr.s_addr = INADDR_ANY; // still listening from any interface
            serv_addr.sin_port = htons( handshakeAddr.port );

        #endif

        if (bind(listen_sck, (struct sockaddr*)&serv_addr,sizeof(serv_addr)) < 0){
            error("Error binding\n");
            return -1;
        }

        if (listen(listen_sck, MAXBACKLOG) < 0){
            error("Error listening\n");
            return -1;
        }
        
        return 0;
    }

    void svc_end() {
        close(this->listen_sck);

        #ifdef LOCAL
            unlink(this->acceptAddr.address.c_str());
        #endif
        
        #ifdef INIT_CUSTOM
        ABT_finalize();
        #endif

    }
    /* 
        Here i should not care of input type nor input data since they come from a socket listener.
        Everything will be handled inside a while true in the body of this node where data is pulled from network
    */
    message_t *svc(message_t* task) {        
        fd_set set, tmpset;
        // intialize both sets (master, temp)
        FD_ZERO(&set);
        FD_ZERO(&tmpset);

        // add the listen socket to the master set
        FD_SET(this->listen_sck, &set);

        // hold the greater descriptor
        int fdmax = this->listen_sck; 

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
                            FD_SET(connfd, &set);
                            if(connfd > fdmax) fdmax = connfd;

                            this->handshakeHandler(connfd);
                            handshakes++;
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

        return this->EOS;
    }

    // Necessary to access internal fields in the RPC callbacks
    friend void ff_rpc(hg_handle_t handle);
    friend void ff_rpc_shutdown(hg_handle_t handle);

protected:
    ff_endpoint                     handshakeAddr;	
    std::vector<ff_endpoint_rpc*>   endRPC;
    size_t                          input_channels;
    std::map<int, int>              routingTable;
	int                             coreid;
    int                             busy;

    size_t                          neos = 0;
    int                             listen_sck;
    int                             last_receive_fd = -1;
    std::vector<margo_instance_id*> mids;
    ABT_pool                        pool_e1;
    ABT_xstream                     xstream_e1;
    size_t                          handshakes = 0;

};

/*
 * An RPC-based receiver node enabled for both internal and external
 * communications on multiple endpoints.
 */
class ff_dreceiverRPCH: public ff_dreceiverRPC {
protected:
    void register_rpcs(margo_instance_id* mid) {
        ff_dreceiverRPC::register_rpcs(mid);

        hg_id_t id = MARGO_REGISTER(*mid, "ff_rpc_internal",
                ff_rpc_in_t, void, ff_rpc_internal);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, this, NULL);

        id = MARGO_REGISTER(*mid, "ff_rpc_shutdown_internal",
                void, void, ff_rpc_shutdown_internal);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, this, NULL);
    }

    void registerEOS(bool internal) {
        ff_dreceiverRPC::registerEOS(internal);
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
            
    }

    virtual int handshakeHandler(const int sck){
        size_t size;
        struct iovec iov; iov.iov_base = &size; iov.iov_len = sizeof(size);
        switch (readvn(sck, &iov, 1)) {
           case -1: error("Error reading from socket\n"); // fatal error
           case  0: return -1; // connection close
        }

        size = be64toh(size);

        char groupName[size];
        if (readn(sck, groupName, size) < 0){
            error("Error reading from socket groupName\n"); return -1;
        }
        
        bool internalGroup = internalGroupsNames.contains(std::string(groupName,size));

        isInternalConnection[sck] = internalGroup; // save somewhere the fact that this sck represent an internal connection

        if (internalGroup) return this->sendRoutingTable(sck, internalDestinations);

        std::vector<int> reachableDestinations;
        for(const auto& [key, value] :  this->routingTable) reachableDestinations.push_back(key);
        return this->sendRoutingTable(sck, reachableDestinations);
    }

    void forward(message_t* task, int sck){
        if (isInternalConnection[sck]) ff_send_out_to(task, this->get_num_outchannels()-1);
        else ff_dreceiverRPC::forward(task, sck);
    }


public:
    //FIXME: in sender you used ff_endpoint_rpc*, here you are using copy not pointer
    // Multi-endpoint extension
    ff_dreceiverRPCH(ff_endpoint handshakeAddr,
        std::vector<ff_endpoint_rpc*> endRPC, size_t input_channels,
        std::map<int, int> routingTable = {{0,0}},
        std::vector<int> internalDestinations = {0},
        std::set<std::string> internalGroupsNames = {},
        int coreid = -1, int busy = 0):
            ff_dreceiverRPC(handshakeAddr, endRPC, input_channels, routingTable,
                coreid, busy),
            internalDestinations(internalDestinations),
            internalGroupsNames(internalGroupsNames) {
        
        // Registering internal version of already initialized mids by base
        // constructor
        for (auto &&mid: this->mids)
        {
            register_rpcs(mid);
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
    friend void ff_rpc_shutdown_internal(hg_handle_t handle);

protected:
    std::vector<int> internalDestinations;
    std::map<int, bool> isInternalConnection;
    std::set<std::string> internalGroupsNames;
    size_t internalNEos = 0, externalNEos = 0;
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

    ff_dreceiverRPCH* receiver =
        (ff_dreceiverRPCH*)margo_registered_data(mid, info->id);

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

    ff_dreceiverRPCH* receiver =
        (ff_dreceiverRPCH*)margo_registered_data(mid, info->id);

    receiver->registerEOS(true);

    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown_internal);