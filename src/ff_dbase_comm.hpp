#ifndef FF_BASE_COMM
#define FF_BASE_COMM

#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <arpa/inet.h>

#include <iostream>
#include <ff/ff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_dgroups.hpp>
#include <ff/distributed/ff_dutils.hpp>

#include <cereal/cereal.hpp>
#include <cereal/archives/portable_binary.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/types/polymorphic.hpp>

#include <abt.h>
#include <margo.h>

#include "ff_drpc_types.h"
#include "ff_margo_utils.hpp"

using namespace ff;

class ff_dAreceiverBase: public ff_monode_t<message_t> {

protected:
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

    virtual void registerEOS(bool) {
        neos++;        
    }

public:

    ff_dAreceiverBase(ff_endpoint handshakeAddr, size_t input_channels,
        std::map<int,int> routingTable, int coreid = -1):
            handshakeAddr(handshakeAddr), input_channels(input_channels),
            routingTable(routingTable), coreid(coreid) {}

    virtual message_t* svc(message_t* task) = 0;

    void svc_end() {
        close(this->listen_sck);

        #ifdef LOCAL
            unlink(this->acceptAddr.address.c_str());
        #endif
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

        int bind_err;
        if ((bind_err = bind(listen_sck, (struct sockaddr*)&serv_addr,sizeof(serv_addr))) < 0){
            error("Error binding: %d -- %s\n", bind_err, strerror(errno));
            return -1;
        }

        if (listen(listen_sck, MAXBACKLOG) < 0){
            error("Error listening\n");
            return -1;
        }
        return 0;
    }


protected:
    ff_endpoint         handshakeAddr;
    size_t              input_channels;
    std::map<int, int>  routingTable;
    int                 coreid;

    size_t              neos = 0;
    int                 listen_sck;
    int                 last_receive_fd;


};


class ff_dAreceiverH : virtual public ff_dAreceiverBase {

protected:
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
        else ff_dAreceiverBase::forward(task, sck);
    }

    virtual void registerEOS(bool internal) {
        if(!internal) {
            if (++externalNEos == (input_channels-internalConnections))
                for(size_t i = 0; i < get_num_outchannels()-1; i++) ff_send_out_to(this->EOS, i);
        } else {
            if (++internalNEos == internalConnections)
                ff_send_out_to(this->EOS, get_num_outchannels()-1);
        }

        ff_dAreceiverBase::registerEOS(internal);
    }

public:

    ff_dAreceiverH(ff_endpoint handshakeAddr, size_t input_channels,
        std::map<int, int> routingTable = {{0,0}},
        std::vector<int> internalDestinations = {0},
        std::set<std::string> internalGroupsNames = {},
        int coreid = -1, int busy = 0):
            ff_dAreceiverBase(handshakeAddr, input_channels, routingTable,
                coreid),
            internalDestinations(internalDestinations),
            internalGroupsNames(internalGroupsNames) {
    
        internalConnections = this->internalGroupsNames.size();
    }

protected:
    std::vector<int>        internalDestinations;
    std::map<int, bool>     isInternalConnection;
    std::set<std::string>   internalGroupsNames;
    size_t                  internalNEos = 0, externalNEos = 0;
    size_t                  internalConnections = 0;
};


class ff_dCommunicator {

public:
    virtual void initializeCommunicator() = 0;
    virtual void listen() = 0;
    virtual void forward() = 0;

};

class ff_dCommunicatorRPC: public ff_dCommunicator {

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
        na_init_info na_info = NA_INIT_INFO_INITIALIZER;
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
        assert(*mid != MARGO_INSTANCE_NULL);
        // margo_set_log_level(*mid, MARGO_LOG_TRACE);

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

public:
    ff_dCommunicatorRPC(int busy = 0): busy(busy) {}

    virtual void initializeCommunicator(std::vector<ff_endpoint_rpc*> &endRPC) {
        init_ABT();
        for (auto &&addr: endRPC)
        {
            margo_instance_id* mid = new margo_instance_id();
            init_mid(addr->margo_addr.c_str(), mid);
            register_rpcs(mid);
            mids.push_back(mid);
        }
    }

    virtual void listen() {
        printf("In svc method\n");      
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
        printf("Waiting now...\n");
        for (auto &&mid : mids)
        {
            ABT_thread* aux = new ABT_thread();
            ABT_thread_create(pool_e1, wait_fin, mid, NULL, aux);
            threads.push_back(aux);
        }

        finalize_xstream_cb(xstream_e1);
        ABT_pool_free(&pool_e1);
        printf("Returning EOS\n");
        return this->EOS;
    }

    friend void ff_rpc(hg_handle_t handle);
    friend void ff_rpc_shutdown(hg_handle_t handle);

protected:
    int                             busy;

    ABT_pool                        pool_e1;
    ABT_xstream                     xstream_e1;
    std::vector<margo_instance_id*> mids;
    size_t                          handshakes = 0;
};

class ff_dreceiverBaseRPC: virtual public ff_dAreceiverBase {

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
        na_init_info na_info = NA_INIT_INFO_INITIALIZER;
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
        assert(*mid != MARGO_INSTANCE_NULL);
        // margo_set_log_level(*mid, MARGO_LOG_TRACE);

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

    virtual void registerEOS(bool internal) {
        ff_dAreceiverBase::registerEOS(internal);
        if(neos == input_channels)
            for (auto &&mid : mids) {
                margo_finalize(*mid);
            }
    } 

public:
    ff_dreceiverBaseRPC(ff_endpoint handshakeAddr,
        std::vector<ff_endpoint_rpc*> endRPC, size_t input_channels,
        std::map<int, int> routingTable = {std::make_pair(0,0)},
        int coreid = -1, int busy = 0):
            ff_dAreceiverBase(handshakeAddr, input_channels, routingTable), 
            endRPC(std::move(endRPC)) {
        
        init_ABT();
        for (auto &&addr: this->endRPC)
        {
            margo_instance_id* mid = new margo_instance_id();
            init_mid(addr->margo_addr.c_str(), mid);
            register_rpcs(mid);
            mids.push_back(mid);
        }
    }


    /* 
        Here i should not care of input type nor input data since they come from a socket listener.
        Everything will be handled inside a while true in the body of this node where data is pulled from network
    */
    message_t *svc(message_t* task) {  
        printf("In svc method\n");      
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
        printf("Waiting now...\n");
        for (auto &&mid : mids)
        {
            ABT_thread* aux = new ABT_thread();
            ABT_thread_create(pool_e1, wait_fin, mid, NULL, aux);
            threads.push_back(aux);
        }

        finalize_xstream_cb(xstream_e1);
        ABT_pool_free(&pool_e1);
        printf("Returning EOS\n");
        return this->EOS;
    }

    friend void ff_rpc(hg_handle_t handle);
    friend void ff_rpc_shutdown(hg_handle_t handle);
    friend void ff_rpc_shutdown_internal(hg_handle_t handle);


protected:
    std::vector<ff_endpoint_rpc*>   endRPC;
    int                             busy;

    ABT_pool                        pool_e1;
    ABT_xstream                     xstream_e1;
    std::vector<margo_instance_id*> mids;
    size_t                          handshakes = 0;

};


class ff_dreceiverHRPC: public ff_dAreceiverH, public ff_dreceiverBaseRPC {

protected:
    void register_rpcs(margo_instance_id* mid) {
        hg_id_t id = MARGO_REGISTER(*mid, "ff_rpc_internal",
                ff_rpc_in_t, void, ff_rpc_internal);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, this, NULL);

        id = MARGO_REGISTER(*mid, "ff_rpc_shutdown_internal",
                void, void, ff_rpc_shutdown_internal);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, this, NULL);
    }

    virtual void registerEOS(bool internal) {
        ff_dAreceiverH::registerEOS(internal);
        if(neos == input_channels)
            for (auto &&mid : mids) {
                margo_finalize(*mid);
            }
    } 

public:
    // Multi-endpoint extension
    ff_dreceiverHRPC(ff_endpoint handshakeAddr,
        std::vector<ff_endpoint_rpc*> endRPC, size_t input_channels,
        std::map<int, int> routingTable = {{0,0}},
        std::vector<int> internalDestinations = {0},
        std::set<std::string> internalGroupsNames = {},
        int coreid = -1, int busy = 0):
            ff_dAreceiverBase(handshakeAddr, input_channels, routingTable, coreid),
            ff_dAreceiverH(handshakeAddr, input_channels, routingTable, internalDestinations, internalGroupsNames, coreid, busy),
            ff_dreceiverBaseRPC(handshakeAddr, endRPC, input_channels, routingTable, coreid, busy) {
        // Registering internal version of already initialized mids by base
        // constructor
        for (auto &&mid: this->mids)
        {
            register_rpcs(mid);
        }
        printf("Finished const\n");
    }

    // Necessary to access internal fields in the RPC callbacks
    friend void ff_rpc_shutdown(hg_handle_t handle);
    friend void ff_rpc_shutdown_internal(hg_handle_t handle);

};

void ff_rpc(hg_handle_t handle) {
    printf("Starting external\n");
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

    ff_dreceiverBaseRPC* receiver =
        (ff_dreceiverBaseRPC*)margo_registered_data(mid, info->id);

    //TODO: change this into a call to forward
    receiver->ff_send_out_to(in.task, receiver->routingTable[in.task->chid]);

    margo_free_input(handle, &in);
    margo_destroy(handle);
    printf("Ending external\n");

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc)


void ff_rpc_internal(hg_handle_t handle) {
    printf("Starting internal\n");

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

    ff_dreceiverHRPC* receiver =
        (ff_dreceiverHRPC*)margo_registered_data(mid, info->id);

    //TODO: change this into a call to forward
    receiver->ff_send_out_to(in.task, receiver->get_num_outchannels()-1);

    margo_free_input(handle, &in);
    margo_destroy(handle);
    printf("Ending internal\n");


    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_internal)


void ff_rpc_shutdown(hg_handle_t handle) {
    printf("Starting shutdown\n");

    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    ff_dreceiverBaseRPC* receiver =
        (ff_dreceiverBaseRPC*)margo_registered_data(mid, info->id);
    
    receiver->registerEOS(false);

    margo_destroy(handle);
    printf("Ending shutdown\n");


    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown);


void ff_rpc_shutdown_internal(hg_handle_t handle) {
    printf("Starting shutinternal\n");

    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    ff_dreceiverHRPC* receiver =
        (ff_dreceiverHRPC*)margo_registered_data(mid, info->id);
    
    receiver->registerEOS(true);
    margo_destroy(handle);
    printf("Ending shutinternal\n");

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown_internal);

#endif