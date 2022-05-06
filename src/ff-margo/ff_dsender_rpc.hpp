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
//TODO: potentially we want to remove the dependence from the original sender
//      and receiver of FastFlow and use this instead.


#include <iostream>
#include <vector>

#include <ff/ff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_dgroups.hpp>

#include <margo.h>
#include <abt.h>

// #include "dist_rpc_type.h"

using namespace ff;


class ff_dsenderRPC: public ff_minode_t<message_t> {
protected:    

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

    int receiveReachableDestinations(int sck, std::map<int,int>& m){
       
        size_t sz;
        ssize_t r;

        if ((r=readn(sck, (char*)&sz, sizeof(sz)))!=sizeof(sz)) {
            if (r==0)
                error("Error unexpected connection closed by receiver\n");
            else			
                error("Error reading size (errno=%d)");
            return -1;
        }
	
        sz = be64toh(sz);

        
        char* buff = new char [sz];
		assert(buff);

        if(readn(sck, buff, sz) < 0){
            error("Error reading from socket\n");
            delete [] buff;
            return -1;
        }

        dataBuffer dbuff(buff, sz, true);
        std::istream iss(&dbuff);
		cereal::PortableBinaryInputArchive iarchive(iss);
        std::vector<int> destinationsList;

        iarchive >> destinationsList;

        for (const int& d : destinationsList) m[d] = sck;

        ff::cout << "Receiving routing table (" << sz << " bytes)" << ff::endl;
        return 0;
    }

    int sendGroupName(const int sck){    
        size_t sz = htobe64(gName.size());
        struct iovec iov[2];
        iov[0].iov_base = &sz;
        iov[0].iov_len = sizeof(sz);
        iov[1].iov_base = (char*)(gName.c_str());
        iov[1].iov_len = gName.size();

        if (writevn(sck, iov, 2) < 0){
            error("Error writing on socket\n");
            return -1;
        }

        return 0;
    }

    virtual int handshakeHandler(const int sck, bool){
        if (sendGroupName(sck) < 0) return -1;

        return receiveReachableDestinations(sck, dest2Socket);
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

    int sendToSck(int sck, message_t* task){
        task->sender = htonl(task->sender);
        task->chid = htonl(task->chid);

        size_t sz = htobe64(task->data.getLen());
        struct iovec iov[4];
        iov[0].iov_base = &task->sender;
        iov[0].iov_len = sizeof(task->sender);
        iov[1].iov_base = &task->chid;
        iov[1].iov_len = sizeof(task->chid);
        iov[2].iov_base = &sz;
        iov[2].iov_len = sizeof(sz);
        iov[3].iov_base = task->data.getPtr();
        iov[3].iov_len = task->data.getLen();

        if (writevn(sck, iov, 4) < 0){
            error("Error writing on socket\n");
            return -1;
        }

        return 0;
    }


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

    void init_mid(const char* proto, margo_instance_id* mid) {
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
            .hg_init_info  = &info      /* struct hg_init_info* */
        };

        // *mid = margo_init_ext(proto, MARGO_CLIENT_MODE, &args);
        *mid = margo_init(proto, MARGO_CLIENT_MODE, 1, -1);
        assert(*mid != MARGO_INSTANCE_NULL);
    }


    void register_rpcs(margo_instance_id* mid) {
        ff_erpc_id = MARGO_REGISTER(*mid, "ff_rpc", ff_rpc_in_t, void, NULL);
        // NOTE: we actually want a response in the non-blocking version
        margo_registered_disable_response(*mid, ff_erpc_id, HG_TRUE);

        ff_eshutdown_id = MARGO_REGISTER(*mid, "ff_rpc_shutdown",
                void, void, NULL);
        margo_registered_disable_response(*mid, ff_eshutdown_id, HG_TRUE);
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

public:
    ff_dsenderRPC(ff_endpoint dest_endpoint, std::vector<ff_endpoint_rpc*> endRPC = {},
        std::string gName = "", int coreid = -1, int busy = 1):
            gName(gName), coreid(coreid),
            endRPC(std::move(endRPC)), busy(busy) {
        this->dest_endpoints.push_back(std::move(dest_endpoint));
    }


    ff_dsenderRPC(std::vector<ff_endpoint> dest_endpoints_,
        std::vector<ff_endpoint_rpc*> endRPC = {},
        std::string gName = "", int coreid=-1, int busy = 1):
            dest_endpoints(std::move(dest_endpoints_)), gName(gName),
            coreid(coreid), endRPC(std::move(endRPC)), busy(busy) {}


    //NOTE: heritage from ff_dsender in order to perform handshake with receiver
    int svc_init() {
        startup();
		if (coreid!=-1)
			ff_mapThreadToCpu(coreid);
		
        sockets.resize(this->dest_endpoints.size());
        for(size_t i=0; i < this->dest_endpoints.size(); i++){
            if ((sockets[i] = tryConnect(this->dest_endpoints[i])) <= 0 ) return -1;
            if (handshakeHandler(sockets[i], false) < 0) return -1;
            sock2End.insert({sockets[i], this->endRPC[i]});
        }

        return 0;
    }

    void svc_end() {
        // close the socket not matter if local or remote
        for(size_t i=0; i < this->sockets.size(); i++)
            close(sockets[i]);

    #ifdef INIT_CUSTOM
        ABT_finalize();
    #endif
    }

    message_t *svc(message_t* task) {
        ff_endpoint_rpc* endp;
        hg_id_t rpc_id;

        if (task->chid == -1){ // roundrobin over the destinations
            task->chid = next_rr_destination;
            next_rr_destination = (next_rr_destination + 1) % dest2Socket.size();
        }
        rpc_id = ff_erpc_id;
        int sck = dest2Socket[task->chid];
        endp = sock2End[sck];

        forwardRequest(task, rpc_id, endp);
        return this->GO_ON;
    }

    void eosnotify(ssize_t id) {
        message_t E_O_S(0,0);
        hg_id_t rpc_id;
        ff_endpoint_rpc* endp;
        if (++neos >= this->get_num_inchannels()){
            for(const auto& sck : sockets) {
                sendToSck(sck, &E_O_S);
                rpc_id = ff_eshutdown_id;
                endp = sock2End[sck];
                forwardRequest(&E_O_S, rpc_id, endp);
            }
        }
    }


protected:

    // From ff_dsender
    size_t                                      neos=0;
    int                                         next_rr_destination = 0;
    std::vector<ff_endpoint>                    dest_endpoints;
    std::map<int, int>                          dest2Socket;
    std::vector<int>                            sockets;
    std::string                                 gName;
    int                                         coreid;

    // Extension for RPC based communication
    std::vector<ff_endpoint_rpc*>               endRPC;
    std::map<std::string, margo_instance_id*>   proto2Margo;
    std::map<int, ff_endpoint_rpc*>             sock2End; 

    hg_id_t                                     ff_erpc_id, ff_eshutdown_id;
    ABT_pool                                    pool_e1;
    ABT_xstream                                 xstream_e1;
    int                                         busy;
};


class ff_dsenderRPCH: public ff_dsenderRPC {

protected:

    // Extended to register internal communication RPCs
    void register_rpcs(margo_instance_id* mid) {
        ff_dsenderRPC::register_rpcs(mid);

        ff_irpc_id = MARGO_REGISTER(*mid, "ff_rpc_internal",
                ff_rpc_in_t, void, NULL);
        margo_registered_disable_response(*mid, ff_irpc_id, HG_TRUE);

        ff_ishutdown_id = MARGO_REGISTER(*mid, "ff_rpc_shutdown_internal",
                void, void, NULL);
        margo_registered_disable_response(*mid, ff_ishutdown_id, HG_TRUE);
    }


public:
    ff_dsenderRPCH(ff_endpoint e, std::vector<ff_endpoint_rpc*> endRPC = {},
        std::string gName = "", std::set<std::string> internalGroups = {},
        int coreid = -1, int busy = 1):
            ff_dsenderRPC(e, endRPC, gName, coreid, busy),
            internalGroups(std::move(internalGroups)) {
        
        // After having initialized the mid instances in the base constructor
        // depending on the different protocols, we register for the same mid
        // the set of RPCs needed for internal communications
        for (auto &el: proto2Margo)
        {
            register_rpcs(el.second);
        }
        
    }
    
    ff_dsenderRPCH(std::vector<ff_endpoint> dest_endpoints_,
        std::vector<ff_endpoint_rpc*> endRPC = {},
        std::string gName = "", std::set<std::string> internalGroups = {},
        int coreid=-1, int busy = 1):
            ff_dsenderRPC(dest_endpoints_, endRPC, gName, coreid, busy),
            internalGroups(std::move(internalGroups)) {
        
        for (auto &el: proto2Margo)
        {
            register_rpcs(el.second);
        }
    }


    int svc_init() {
        startup();
       
        if (coreid!=-1)
			ff_mapThreadToCpu(coreid);

        sockets.resize(this->dest_endpoints.size());
        for (size_t i = 0; i < this->dest_endpoints.size(); i++)
        {
            int sck = tryConnect(this->dest_endpoints[i]);
            if (sck <= 0) {
                error("Error on connecting!\n");
                return -1;
            }

            bool isInternal = internalGroups.contains(dest_endpoints[i].groupName);
            if (isInternal) internalSockets.push_back(sck);
            else sockets.push_back(sck);
            //NOTE: probably this can be substituted by creating the handle for
            //      the RPC as it is done exactly before the forwarding process
            //      the saved handle can then be used in the communications
            //      involving this "socket" descriptor
            handshakeHandler(sck, isInternal);
            sock2End.insert({sck, endRPC[i]});
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

            forwardRequest(task, rpc_id, endp);
            return this->GO_ON;
        }

        ff_dsenderRPC::svc(task);
        return this->GO_ON;
    }

    void svc_end() {
        for (auto &&mid : proto2Margo)
        {
            margo_finalize(*mid.second);
        }
        finalize_xstream_cb(xstream_e1);
        ABT_pool_free(&pool_e1);
        
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
        ff_dsenderRPC::eosnotify(id);
     }


protected:
    std::vector<int>                            internalSockets;
    std::set<std::string>                       internalGroups;
    std::map<int, int>::const_iterator          rr_iterator;
    std::map<int, int>                          internalDest2Socket;

    hg_id_t                                     ff_irpc_id, ff_ishutdown_id;
};