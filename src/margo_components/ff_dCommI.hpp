/* This is a generic interface used to provide protocol-specific functionalities
to remotely connected FastFlow's nodes. It must be extended in order to implement
the barely necessary functions to receive and ship data in the network. */

#ifndef FF_DCOMM_I
#define FF_DCOMM_I

#include <map>
#include <ff/ff.hpp>
#include <ff/dff.hpp>
#include <ff/distributed/ff_network.hpp>
#include "../ff_dCompI.hpp"

using namespace ff;

class ff_dCommunicatorRPC: public ff_dComp {
protected:

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

public:
    virtual int comm_listen() {
        return this->_listen();
    }
    
    size_t getInternalConnections(){
        return this->internalConnections;
    }

protected:
    ff_dCommunicatorRPC(ff_endpoint handshakeAddr, size_t input_channels):
        ff_dComp(input_channels), handshakeAddr(handshakeAddr) {}

    ff_endpoint             handshakeAddr;
    std::map<int, ChannelType> sck2ChannelType;

    int                     listen_sck;
    int                     last_receive_fd = -1;
    int                     fdmax;
    fd_set                  set, tmpset;
};


class ff_dCommunicatorRPCS: public ff_dCompS {
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


public:
    virtual void init() = 0;
    virtual int send(message_t* task, bool external) = 0;
    virtual void finalize() {
        for(size_t i=0; i < this->sockets.size(); i++)
            close(sockets[i]);

        for(size_t i=0; i<this->internalSockets.size(); i++) {
            close(internalSockets[i]);
        }
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
        return 0;
    }

    virtual void notify(ssize_t id, bool external) = 0;

    bool haveConnType(bool external) {
        return external ? haveExternal : haveInternal;
    }

protected:
    ff_dCommunicatorRPCS(std::pair<ChannelType, ff_endpoint> destEndpoint,
        std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF):
            ff_dCompS(destEndpoint, gName, batchSize, messageOTF, internalMessageOTF){
    }


    ff_dCommunicatorRPCS(std::vector<std::pair<ChannelType,ff_endpoint>> destEndpoints_,
        std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF):
            ff_dCompS(destEndpoints_, gName, batchSize, messageOTF, internalMessageOTF){}


    std::vector<int>                                    sockets;
    std::map<std::pair<int, ChannelType>, int>          dest2Socket;
    std::vector<int>                                    internalSockets;
    int externalDests = 0, internalDests = 0;
    int nextExternal = 0, nextInternal = 0;
};

#endif
