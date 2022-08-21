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
//TODO: controllare tutte le variabili aggiornate all'interno delle RPC perché
//      potrei avere delle race condition
#ifndef FF_DASENDER
#define FF_DASENDER

#include <iostream>
#include <vector>
#include <thread>

#include <ff/ff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_dgroups.hpp>

#include <margo.h>
#include <abt.h>

#include "ff_drpc_types.h"
#include "ff_margo_utils.hpp"
//TODO: check for delete calls missing in svc method or some protocol retrieval
//      via strdup

using namespace ff;

#ifndef FF_DCOMMS
#define  FF_DCOMMS
class ff_dCommunicatorS {

protected:
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


    virtual int handshakeHandler(const int sck, bool isInternal){
        if (sendGroupName(sck) < 0) return -1;

        return receiveReachableDestinations(sck, isInternal ? internalDest2Socket : dest2Socket);
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

public:
    virtual void init() = 0;
    virtual void send(message_t* task, bool external) = 0;
    virtual void finalize() {
        for(size_t i=0; i < this->sockets.size(); i++)
            close(sockets[i]);

        for(size_t i=0; i<this->internalSockets.size(); i++) {
            close(internalSockets[i]);
        }
    }

    virtual int handshake() {
        for (size_t i = 0; i < this->dest_endpoints.size(); i++)
        {
            std::cout << "Trying to connect to: " << this->dest_endpoints[i].address << "\n";
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
            socks.push_back(sck);
        }
        return 0;
    }

    int getInternalCount() {
        return this->internalDest2Socket.size();
    }

    int getExternalCount() {
        return this->dest2Socket.size();
    }

protected:
    ff_dCommunicatorS(ff_endpoint dest_endpoint,
        std::string gName = "", std::set<std::string> internalGroups = {}):
            gName(gName), internalGroups(std::move(internalGroups)){
        
        this->dest_endpoints.push_back(std::move(dest_endpoint));
    }


    ff_dCommunicatorS(std::vector<ff_endpoint> dest_endpoints_,
        std::string gName = "", std::set<std::string> internalGroups = {}):
            dest_endpoints(std::move(dest_endpoints_)), gName(gName),
            internalGroups(std::move(internalGroups)) {}


    std::vector<ff_endpoint>                dest_endpoints;
    std::string                             gName;
    std::set<std::string>                   internalGroups;
    std::vector<int>                        sockets;
    std::map<int, int>                      dest2Socket;
    std::vector<int>                        internalSockets;
    std::map<int, int>                      internalDest2Socket;
    std::vector<int>                        socks;
};
#endif


class ff_dAsender: public ff_minode_t<message_t> {
public:
    ff_dAsender(ff_dCommunicatorS* communicator, int coreid = -1, int busy = 1):
            communicator(communicator), coreid(coreid), busy(busy) {
        
        this->communicator->init();
    }


    int svc_init() {
		if (coreid!=-1)
			ff_mapThreadToCpu(coreid);
		
        //DESIGN: only because we still do not separate init abt from main thread
        if (communicator->handshake() == -1) {
            error("Handhsake error.\n");
            return -1;
        }

        this->externalDests = communicator->getExternalCount();
        this->internalDests = communicator->getInternalCount();

        return 0;
    }

    void svc_end() {
        communicator->finalize();
    }

    message_t *svc(message_t* task) {
        // Conditionally retrieve endpoint information and RPC id based on
        // internal/external chid.
        if (internalDests > 0
            && (this->get_channel_id() == (ssize_t)(this->get_num_inchannels() - 1))){
            
            printf("I'm sending an internal message!\n");
            // pick destination from the list of internal connections!
            if (task->chid == -1){ // roundrobin over the destinations
                task->chid = nextInternal;
                nextInternal = (nextInternal + 1) % internalDests;
            }

            communicator->send(task, false);
            return this->GO_ON;
        }

        if (task->chid == -1){ // roundrobin over the destinations
            task->chid = nextExternal;
            nextExternal = (nextExternal + 1) % externalDests;
        }

        communicator->send(task, true);
        // delete task;

        return this->GO_ON;
    }

    void eosnotify(ssize_t id) {

        if (id == (ssize_t)(this->get_num_inchannels() - 1)){
            std::cout << "Received EOS message from RBox!\n";
            message_t E_O_S(0,0);
            // send the EOS to all the internal connections
            communicator->send(&E_O_S, false);                           
        }

        if (++neos >= this->get_num_inchannels()) {
            message_t E_O_S(0,0);
            communicator->send(&E_O_S, true);
        }
    }

protected:

    // From ff_dsender
    ff_dCommunicatorS*          communicator;
    size_t                      neos=0;
    int                         nextExternal = 0, nextInternal = 0;
    int                         coreid;
    int                         busy;

    int                         externalDests = 0, internalDests = 0;
};

#endif