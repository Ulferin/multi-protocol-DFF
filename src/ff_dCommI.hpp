/* This is a generic interface used to provide protocol-specific functionalities
to remotely connected FastFlow's nodes. It must be extended in order to implement
the barely necessary functions to receive and ship data in the network. */

#ifndef FF_DCOMM_I
#define FF_DCOMM_I

class ff_dCommunicator {
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

        if(internal) {
            bool internalGroup = internalGroupsNames.contains(std::string(groupName,size));
            isInternalConnection[sck] = internalGroup; // save somewhere the fact that this sck represent an internal connection

            if (internalGroup) return this->sendRoutingTable(sck, internalDestinations);
        }

        std::vector<int> reachableDestinations;
        for(const auto& [key, value] : this->routingTable)
            reachableDestinations.push_back(key);

        return this->sendRoutingTable(sck, reachableDestinations);
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
            error("Error binding: %d -- %s\n", bind_err, strerror(errno));
            return -1;
        }

        if (listen(listen_sck, MAXBACKLOG) < 0){
            error("Error listening\n");
            return -1;
        }

        return 0;
    }

    int _listen() {
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
    virtual void init(ff_monode_t<message_t>*, int) = 0;
    virtual int comm_listen() {
        return this->_listen();
    }
    virtual void finalize() = 0;
    
    int getChannelID(int chid) {
        return this->routingTable[chid];
    }

    size_t getInternalConnections(){
        return this->internalConnections;
    }

protected:
    ff_dCommunicator(ff_endpoint handshakeAddr, bool internal,
        std::vector<int> internalDestinations = {0},
        std::map<int, int> routingTable = {{0,0}},
        std::set<std::string> internalGroupsNames = {}):
        handshakeAddr(handshakeAddr), internal(internal),
        internalDestinations(internalDestinations),
        routingTable(routingTable),
        internalGroupsNames(internalGroupsNames)
    {
        internalConnections = this->internalGroupsNames.size();
    }

    ff_endpoint             handshakeAddr;
    bool                    internal;
    std::vector<int>        internalDestinations;
    std::map<int, int>      routingTable;
    std::set<std::string>   internalGroupsNames;
    size_t                  internalConnections = 0;
    std::map<int, bool>     isInternalConnection;

    int                     listen_sck;
    size_t                  handshakes = 0;
    size_t                  input_channels;
    int                     last_receive_fd = -1;
    int                     fdmax;
    fd_set                  set, tmpset;
};


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

    std::map<int, int> getInternalMap() {
        return this->internalDest2Socket;
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
    std::map<int, int>                      dest2Socket;        //FIXME: poor naming, this is simply dest2handle
    std::vector<int>                        internalSockets;    //FIXME: same
    std::map<int, int>                      internalDest2Socket;//FIXME: same
    std::vector<int>                        socks;  //CHECK: is it really used?
};
#endif