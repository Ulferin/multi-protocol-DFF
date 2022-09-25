#ifndef FF_DCOMM
#define FF_DCOMM

#include <ff/ff.hpp>
#include <ff/dff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_batchbuffer.hpp>
#include <margo.h>

#include "../ff_dTransportTypeI.hpp"
#include "../ff_dAreceiverComp.hpp"
#include "ff_drpc_types.h"
#include "ff_margo_utils.hpp"

class TransportRPC: public TransportType {

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
        hg_id_t id = MARGO_REGISTER(*mid, "ff_rpc", ff_rpc_in_t, void, ff_rpc);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, data, NULL);

        id = MARGO_REGISTER(*mid, "ff_rpc_shutdown",
                ff_rpc_shutdown_in_t, void, ff_rpc_shutdown);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, data, NULL);
    }


    virtual int handshakeHandler(const int sck){

        // ricevo l'handshake e mi salvo che tipo di connessione è
        size_t size;
        ChannelType t;
        struct iovec iov[2]; 
        iov[0].iov_base = &t; iov[0].iov_len = sizeof(ChannelType);
        iov[1].iov_base = &size; iov[1].iov_len = sizeof(size);
        switch (readvn(sck, iov, 2)) {
           case -1: error("Error reading from socket\n"); // fatal error
           case  0: return -1; // connection close
        }

        size = be64toh(size);

        char groupName[size];
        if (readn(sck, groupName, size) < 0){
            error("Error reading from socket groupName\n"); return -1;
        }
        
        sck2ChannelType[sck] = t;
        if(t == ChannelType::INT) this->internalConnections++;
        std::cout << "Done handshake with " << groupName << " - internal: " << this->internalConnections << "\n";
        return 0;
    }


    int _init() {
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

        int bind_err;
        if ((bind_err = bind(listen_sck, (struct sockaddr*)&serv_addr,sizeof(serv_addr))) < 0){
            error("Error binding port %d: %d -- %s\n", handshakeAddr.port, bind_err, strerror(errno));
            return -1;
        }

        if (listen(listen_sck, MAXBACKLOG) < 0){
            error("Error listening\n");
            return -1;
        }

        return 0;
    }

    virtual int _listen() {
        if (this->_init() == -1) {
            error("Error initializing communicator\n");
            return -1;
        }

        // intialize both sets (master, temp)
        FD_ZERO(&set);
        FD_ZERO(&tmpset);

        // add the listen socket to the master set
        FD_SET(this->listen_sck, &set);

        // hold the greater descriptor
        fdmax = this->listen_sck; 

        while(handshakes < input_channels){
            // copy the master set to the temporary
            tmpset = set;

            switch(select(fdmax+1, &tmpset, NULL, NULL, NULL)){
                case -1: error("Error on selecting socket\n"); return -1;
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
        return 0;
    }

    void boot_component() {
        init_ABT();
        for (auto &&addr: this->endRPC)
        {
            margo_instance_id* mid = new margo_instance_id();
            init_mid(addr->margo_addr.c_str(), mid);
            mids.push_back(mid);
        }
    }

public:
    TransportRPC(ff_endpoint handshakeAddr, size_t input_channels,
        std::vector<ff_endpoint_rpc*> endRPC, bool internal=false, bool busy=true):
        TransportType(input_channels), handshakeAddr(handshakeAddr),
        endRPC(std::move(endRPC)), internal(internal), busy(busy) {
            this->boot_component();
        }

    virtual void init(ff_monode_t<message_t> *data) {
        for (auto &&mid: mids)
        {
            register_rpcs(mid, data);
        }
    }

    virtual int comm_listen() {
        if(this->_listen() == -1)
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

    ff_endpoint             handshakeAddr;
    std::map<int, ChannelType> sck2ChannelType;

    int                     listen_sck;
    int                     last_receive_fd = -1;
    int                     fdmax;
    fd_set                  set, tmpset;
};


class TransportRPCS: public TransportTypeS {

protected:
    virtual int handshakeHandler(const int sck, ChannelType ct){
        size_t sz = htobe64(gName.size());
        struct iovec iov[3];
        iov[0].iov_base = &ct;
        iov[0].iov_len = sizeof(ChannelType);
        iov[1].iov_base = &sz;
        iov[1].iov_len = sizeof(sz);
        iov[2].iov_base = (char*)(gName.c_str());
        iov[2].iov_len = gName.size();

        if (writevn(sck, iov, 3) < 0){
            error("Error writing on socket\n");
            return -1;
        }

        return 0;
    }


    int create_connect(const ff_endpoint& destination){
        int socketFD;

        #ifdef LOCAL
            socketFD = socket(AF_LOCAL, SOCK_STREAM, 0);
            if (socketFD < 0){
                error("\nError creating socket \n");
                return socketFD;
            }
            struct sockaddr_un serv_addr;
            memset(&serv_addr, '0', sizeof(serv_addr));
            serv_addr.sun_family = AF_LOCAL;

            strncpy(serv_addr.sun_path, destination.address.c_str(), destination.address.size()+1);

            if (connect(socketFD, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
                close(socketFD);
                return -1;
            }
        #endif

        #ifdef REMOTE
            struct addrinfo hints;
            struct addrinfo *result, *rp;

            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_UNSPEC;    /* Allow IPv4 or IPv6 */
            hints.ai_socktype = SOCK_STREAM; /* Stream socket */
            hints.ai_flags = 0;
            hints.ai_protocol = IPPROTO_TCP;          /* Allow only TCP */

            // resolve the address 
            if (getaddrinfo(destination.address.c_str() , std::to_string(destination.port).c_str() , &hints, &result) != 0)
                return -1;

            // try to connect to a possible one of the resolution results
            for (rp = result; rp != NULL; rp = rp->ai_next) {
               socketFD = socket(rp->ai_family, rp->ai_socktype,
                            rp->ai_protocol);
               if (socketFD == -1)
                   continue;

               if (connect(socketFD, rp->ai_addr, rp->ai_addrlen) != -1)
                   break;                  /* Success */

               close(socketFD);
           }
		   free(result);
			
           if (rp == NULL)            /* No address succeeded */
               return -1;
        #endif


        // receive the reachable destination from this sockets

        return socketFD;
    }


    int tryConnect(const ff_endpoint &destination){
        int fd = -1, retries = 0;

        while((fd = this->create_connect(destination)) < 0 && ++retries < MAX_RETRIES)
            if (retries < AGGRESSIVE_TRESHOLD)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            else
                std::this_thread::sleep_for(std::chrono::milliseconds(200));

        return fd;
    }

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
                ff_rpc_shutdown_in_t, void, NULL);
        margo_registered_disable_response(*mid, ff_eshutdown_id, HG_TRUE);
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

    void forwardRequest(message_t* task, hg_bool_t external, ff_endpoint_rpc* endp) {
        ff_rpc_in_t in;
        in.task = new message_t(task->data.getPtr(), task->data.getLen(), true);
        in.task->chid = task->chid;
        in.task->sender = task->sender;
        in.external = external;

        hg_handle_t h = shipRPC(endp, ff_erpc_id);
        margo_forward(h, &in);
        margo_destroy(h);
        
        delete in.task;
    }

    void forwardEOS(message_t* task, hg_bool_t external, ff_endpoint_rpc* endp) {
        ff_rpc_shutdown_in_t in;
        in.external = external;
        hg_handle_t h = shipRPC(endp, ff_eshutdown_id);
        margo_forward(h, &in);
        margo_destroy(h);
    }

public:

    TransportRPCS(std::pair<ChannelType, ff_endpoint> destEndpoint,
        std::vector<ff_endpoint_rpc*> endRPC,
        std::string gName = "", bool internal=false, bool busy = true):
            TransportTypeS(destEndpoint, gName, batchSize, messageOTF, internalMessageOTF), busy(busy),
            endRPC(std::move(endRPC)), internal(internal) {
                this->boot_component();
            }

    TransportRPCS(std::vector<std::pair<ChannelType,ff_endpoint>> destEndpoints,
        std::vector<ff_endpoint_rpc*> endRPC,
        std::string gName = "", bool internal=false, bool busy = true):
            TransportTypeS(destEndpoints, gName, batchSize, messageOTF, internalMessageOTF), busy(busy),
            endRPC(std::move(endRPC)), internal(internal) {
                this->boot_component();
            }

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

    virtual int send(message_t* task, bool external) {
        ff_endpoint_rpc* endp;
        hg_id_t rpc_id;
        count++;
        int sck;
        if (task->chid == -1)
            task->chid = getNextSck(external);
            
        if(external) sck = dest2Socket[{task->chid, task->feedback ? ChannelType::FBK : ChannelType::FWD}]; 
        else sck = dest2Socket[{task->chid, ChannelType::INT}];
        
        endp = sock2End[sck];
        forwardRequest(task, external, endp);

        return 0;
    }

    virtual void notify(ssize_t id, bool external) {
        if(external) 
            for (auto& sck : sockets){
                forwardRequest(new message_t(id, -2), external, sock2End[sck]);
            }
        else {
            // send the EOS to all the internal connections
            for(const auto& sck : internalSockets){
                forwardEOS(NULL, false, sock2End[sck]);
            }
        }
    }

    virtual void finalize() {
        for(size_t i=0; i < this->sockets.size(); i++)
            close(sockets[i]);

        for(size_t i=0; i<this->internalSockets.size(); i++) {
            close(internalSockets[i]);
        }

        for(auto& sck : sockets){
            forwardEOS(NULL, true, sock2End[sck]);
        }

        for (auto &&mid : proto2Margo)
        {
            margo_finalize(*mid.second);
        }
        finalize_xstream_cb(xstream_e1);
        ABT_pool_free(&pool_e1);
    }


    virtual int handshake(precomputedRT_t* rt) {
        for (auto& [ct, ep] : this->destEndpoints)
        {
            std::cout << "Trying to connect to: " << ep.address << "\n";
            int sck = tryConnect(ep);
            if (sck <= 0) {
                error("Error on connecting!\n");
                return -1;
            }

            bool isInternal = ct == ChannelType::INT;
            haveInternal = haveInternal || isInternal;
            haveExternal = haveExternal || !isInternal;
            if(isInternal) {internalDests++; internalSockets.push_back(sck);}
            else {externalDests++; sockets.push_back(sck);}
            socks.push_back(sck);

            // compute the routing table!
            for(auto& [k,v] : *rt){
                if (k.first != ep.groupName) continue;
                for(int dest : v) {
                    dest2Socket[std::make_pair(dest, k.second)] = sck;
                    printf("[HSK]Putting %d:%d -> %d\n", dest, k.second, sck);
                }
            }

            if(handshakeHandler(sck, ct) < 0) {
                error("Error in handshake");
                return -1;
            }
        }
        this->destEndpoints.clear();

        rr_iterator = dest2Socket.cbegin();

        for (size_t i = 0; i < socks.size(); i++) {
            sock2End.insert({socks[i], this->endRPC[i]});
        }

        return 0;
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

    std::vector<int>                            sockets;
    std::map<std::pair<int, ChannelType>, int>  dest2Socket;
    std::vector<int>                            internalSockets;
    int                                         externalDests=0, internalDests=0;
    int                                         nextExternal=0, nextInternal=0;

    std::map<std::pair<int, ChannelType>, int>::const_iterator  rr_iterator;
    int                                         count = 0;
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

    ff_dAreceiver* receiver =
        (ff_dAreceiver*)margo_registered_data(mid, info->id);

    receiver->received++;
    if(in.task->chid == -2)
        receiver->registerLogicalEOS(in.task->sender);
    receiver->forward(in.task, !(in.external));

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc)


void ff_rpc_shutdown(hg_handle_t handle) {
    hg_return_t             hret;
    const struct hg_info*   info;
    margo_instance_id       mid;
    ff_rpc_shutdown_in_t    in;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    ff_dAreceiver* receiver =
        (ff_dAreceiver*)margo_registered_data(mid, info->id);
    
    receiver->registerEOS(!(in.external));
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown);


#endif //FFDCOMM
