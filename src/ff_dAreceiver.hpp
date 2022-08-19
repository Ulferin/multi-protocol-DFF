#ifndef FF_DARECEIVER
#define FF_DARECEIVER

#include <iostream>
#include <vector>
#include <map>

#include <ff/ff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_dgroups.hpp>
#include <ff/distributed/ff_dutils.hpp>
#include <cereal/cereal.hpp>
#include <cereal/archives/portable_binary.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/types/polymorphic.hpp>



using namespace ff;

//DESIGN: we actually want to put this somewhere else, probably inside the
//      networking part of FastFlow's library

/* This is a generic interface used to provide protocol-specific functionalities
to remotely connected FastFlow's nodes. It must be extended in order to implement
the barely necessary functions to receive and ship data in the network. */
#ifndef FF_DCOMM
#define  FF_DCOMM

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

    //DESIGN: insert into defined listen and then call the original listen + the implemented one
    //      like ff_dCommunicator::listen(); this->listen();
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
                            // close(connfd); //DESIGN: check this in TCP plugin
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
    virtual void send() = 0;
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
#endif


class ff_dAreceiver: public ff_monode_t<message_t> {


public:

    //DESIGN: check communicator as pointer instead of as reference; moreover,
    //      should we move the communicator with std::move?
    ff_dAreceiver(ff_dCommunicator* communicator,
        size_t input_channels, int coreid = -1, int busy = 0):
            communicator(communicator), input_channels(input_channels),
            coreid(coreid), busy(busy)
    {
        //DESIGN: check if this call actually register the base class or the
        //      extended one. In the AreceiverH we are calling the base constructor
        //      to not repeat the init call.
        this->communicator->init(this, input_channels);
    }


    int svc_init() {
  		if (coreid!=-1)
			ff_mapThreadToCpu(coreid);
        
        return 0;
    }


    message_t *svc(message_t* task) {        
        //NOTE: chiamata communicator specific, qui va controllato come gestire
        //      gli altri protocolli. Il communicator TCP potrebbe aver bisogno
        //      della lista dei socket connessi durante handshake.
        if(communicator->comm_listen() == -1) {
            error("Listening for messages\n");
            return this->EOS;
        }
        return this->EOS;
    }


    virtual void forward(message_t* task, bool internal){
        if (internal) ff_send_out_to(task, this->get_num_outchannels()-1);
        else ff_send_out_to(task, communicator->getChannelID(task->chid)); // assume the routing table is consistent WARNING!!!
    }


    virtual void registerEOS(bool internal) {

        printf("Internal: %d -- in_ch: %ld / in_conn: %ld -- ext_eos: %ld / in_eos: %ld\n", internal, input_channels, communicator->getInternalConnections(), externalNEos, internalNEos);
        if(!internal) {
            if (++this->externalNEos == (this->input_channels-communicator->getInternalConnections()))
                for(size_t i = 0; i < get_num_outchannels()-1; i++) ff_send_out_to(this->EOS, i);
        } else {
            if (++this->internalNEos == communicator->getInternalConnections())
                ff_send_out_to(this->EOS, get_num_outchannels()-1);
        }

        if(++neos == input_channels)
            communicator->finalize();   
    }

protected:
    ff_dCommunicator*   communicator;
    size_t              input_channels;
    int                 coreid;
    int                 busy;

    size_t              neos = 0;
    size_t              internalNEos = 0, externalNEos = 0;

};

#endif