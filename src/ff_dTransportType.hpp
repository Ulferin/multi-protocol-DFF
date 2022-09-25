#ifndef FF_DCOMM_COMP
#define FF_DCOMM_COMP

#include <ff/ff.hpp>
#include <ff/dff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_batchbuffer.hpp>
#include <mpi.h>
#include "ff_dTransportTypeI.hpp"
#include "ff_dAreceiverComp.hpp"

using namespace ff;

#ifndef DFF_EXCLUDE_MPI
class TransportMPI: public TransportType {
protected:
    ff_dAreceiver* receiver;        // FIXME: this should become a callback instead of whole object
    std::set<int> internalRanks;
    std::map<int, ChannelType> rank2ChannelType;
    bool finalized = false;
    size_t neos = 0;

public:
    TransportMPI(size_t input_channels)
		: TransportType(input_channels) {}

    virtual void init(ff_monode_t<message_t>* data) {
        receiver = (ff_dAreceiver*)data;

        printf("Starting handshake with MPI\n");
        for(size_t i = 0; i < input_channels; i++)
            handshakeHandler();
        printf("Ending handshake with MPI\n");
        
        this->internalConnections = std::count_if(std::begin(rank2ChannelType),
                                            std::end  (rank2ChannelType),
                                            [](std::pair<int, ChannelType> const &p) {return p.second == ChannelType::INT;});
    }

    virtual int comm_listen() {
        MPI_Status status;
        if(neos < input_channels){
            
            int headersLen;
            int flag = 0;
            // MPI_Probe(MPI_ANY_SOURCE, DFF_HEADER_TAG, MPI_COMM_WORLD, &status);
            MPI_Iprobe(MPI_ANY_SOURCE, DFF_HEADER_TAG, MPI_COMM_WORLD, &flag, &status);
            if(flag == 0)
                return 1;
            MPI_Get_count(&status, MPI_LONG, &headersLen);
            long headers[headersLen];
            int rank = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
            
            if (MPI_Recv(headers, headersLen, MPI_LONG, status.MPI_SOURCE, DFF_HEADER_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS)
                error("Error on Recv Receiver primo in alto\n");
            bool feedback = ChannelType::FBK == rank2ChannelType[status.MPI_SOURCE];
            assert(headers[0]*3+1 == headersLen);
            if (headers[0] == 1) {
                size_t sz = headers[3];

                if (sz == 0){
                    if (headers[2] == -2){
                        receiver->registerLogicalEOS(headers[1]);
                        return 1;
                    }
                    neos++;
                    receiver->registerEOS(rank2ChannelType[status.MPI_SOURCE] == ChannelType::INT);
                    return 1;
                }

                char* buff = new char[sz];
                if (MPI_Recv(buff,sz,MPI_BYTE, status.MPI_SOURCE, DFF_TASK_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE) != MPI_SUCCESS)
                    error("Error on Recv Receiver Payload\n");
                receiver->received++;
                message_t* out = new message_t(buff, sz, true);
                out->sender = headers[1];
                out->chid   = headers[2];
                out->feedback = feedback;

                receiver->forward(out, rank2ChannelType[status.MPI_SOURCE] == ChannelType::INT);
            } else {
                int size;
                MPI_Status localStatus;
                MPI_Probe(status.MPI_SOURCE, DFF_TASK_TAG, MPI_COMM_WORLD, &localStatus);
                MPI_Get_count(&localStatus, MPI_BYTE, &size);
                char* buff = new char[size]; // this can be reused!! 
                MPI_Recv(buff, size, MPI_BYTE, localStatus.MPI_SOURCE, DFF_TASK_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                size_t head = 0;
                for (size_t i = 0; i < (size_t)headers[0]; i++){
                    size_t sz = headers[3*i+3];
                    if (sz == 0){
                        if (headers[3*i+2] == -2){
                            receiver->registerLogicalEOS(headers[3*i+1]);
                            return 1;
                        }
                        receiver->registerEOS(rank2ChannelType[status.MPI_SOURCE] == ChannelType::INT);
                        assert(i+1 == (size_t)headers[0]);
                        break;
                    }
                    char* outBuff = new char[sz];
                    memcpy(outBuff, buff+head, sz);
                    head += sz;
                    message_t* out = new message_t(outBuff, sz, true);
                    out->sender = headers[3*i+1];
                    out->chid = headers[3*i+2];
                    out->feedback = feedback;

                    receiver->forward(out, rank2ChannelType[status.MPI_SOURCE] == ChannelType::INT);
                }
                delete [] buff;
            }
            return 1;
        }
        
        return 0;
    }

    virtual void finalize() {
        printf("Finalizing communicator.\n");
        this->finalized = true;
    }

protected:
    virtual int handshakeHandler(){
        int sz;
        ChannelType ct;
        MPI_Status status;
        MPI_Probe(MPI_ANY_SOURCE, DFF_GROUP_NAME_TAG, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_BYTE, &sz);
        char* buff = new char [sz];
        MPI_Recv(buff, sz, MPI_BYTE, status.MPI_SOURCE, DFF_GROUP_NAME_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&ct, sizeof(ct), MPI_BYTE, status.MPI_SOURCE, DFF_CHANNEL_TYPE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        rank2ChannelType[status.MPI_SOURCE] = ct;

        return 0;
    }
    
};

class TransportMPIS: public TransportTypeS {

protected:
    class batchBuffer {
    protected:    
        int rank;
        bool blocked = false;
        size_t size_, actualSize = 0;
        std::vector<char> buffer;
        std::vector<long> headers;
        MPI_Request headersR, datasR;
    public:
        batchBuffer(size_t size_, int rank) : rank(rank), size_(size_){
            headers.reserve(size_*3+1);
        }
        virtual void waitCompletion(){
            if (blocked){
                MPI_Wait(&headersR, MPI_STATUS_IGNORE);
                MPI_Wait(&datasR, MPI_STATUS_IGNORE);
                headers.clear();
                buffer.clear();
                blocked = false;
            }
        }

        virtual size_t size() {return actualSize;}
        virtual int push(message_t* m){
            waitCompletion();
            int idx = 3*actualSize++;
            headers[idx+1] = m->sender;
            headers[idx+2] = m->chid;
            headers[idx+3] = m->data.getLen();

            buffer.insert(buffer.end(), m->data.getPtr(), m->data.getPtr() + m->data.getLen());

            delete m;
            if (actualSize == size_) {
                this->flush();
                return 1;
            }
            return 0;
        }

        virtual void flush(){
            headers[0] = actualSize;
            MPI_Isend(headers.data(), actualSize*3+1, MPI_LONG, rank, DFF_HEADER_TAG, MPI_COMM_WORLD, &headersR);
            MPI_Isend(buffer.data(), buffer.size(), MPI_BYTE, rank, DFF_TASK_TAG, MPI_COMM_WORLD, &datasR);
            blocked = true;
            actualSize = 0;
        }

        virtual void pushEOS(){
            int idx = 3*actualSize++;
            headers[idx+1] = 0; headers[idx+2] = 0; headers[idx+3] = 0;

            this->flush();
        }
    };

    class directBatchBuffer : public batchBuffer {
            message_t* currData = NULL;
            long currHeader[4] = {1, 0, 0, 0}; 
        public:
            directBatchBuffer(int rank) : batchBuffer(0, rank){}
            void pushEOS(){
                waitCompletion();
                currHeader[1] = 0; currHeader[2] = 0; currHeader[3] = 0;
                MPI_Send(currHeader, 4, MPI_LONG, this->rank, DFF_HEADER_TAG, MPI_COMM_WORLD);
            }
            void flush() {}
            void waitCompletion(){
                if (blocked){
                    MPI_Wait(&headersR, MPI_STATUS_IGNORE);
                    if (currData->data.getLen() > 0)
                        MPI_Wait(&datasR, MPI_STATUS_IGNORE);
                    if (currData) delete currData;
                    blocked = false;
                }
            }
            int push(message_t* m){
                waitCompletion();
                currHeader[1] = m->sender; currHeader[2] = m->chid; currHeader[3] = m->data.getLen();
                MPI_Isend(currHeader, 4, MPI_LONG, this->rank, DFF_HEADER_TAG, MPI_COMM_WORLD, &this->headersR);
                if (m->data.getLen() > 0)
                    MPI_Isend(m->data.getPtr(), m->data.getLen(), MPI_BYTE, rank, DFF_TASK_TAG, MPI_COMM_WORLD, &datasR);
                currData = m;
                blocked = true;
                return 1;
            }
    };

    virtual int handshakeHandler(const int rank, ChannelType ct){
        MPI_Send(gName.c_str(), gName.size(), MPI_BYTE, rank, DFF_GROUP_NAME_TAG, MPI_COMM_WORLD);
        MPI_Send(&ct, sizeof(ChannelType), MPI_BYTE, rank, DFF_CHANNEL_TYPE_TAG, MPI_COMM_WORLD);
        return 0;
    }

    int getMostFilledInternalBufferRank(){
        int rankMax = -1;
        size_t sizeMax = 0;
        for(int rank : internalRanks){
            auto& batchBB = buffers[rank];
            size_t sz = batchBB.second[batchBB.first]->size();
            if (sz > sizeMax) {
                rankMax = rank;
                sizeMax = sz;
            }
        }
        if (rankMax >= 0) return rankMax;

        last_rr_rank_Internal = (last_rr_rank_Internal + 1) % this->internalRanks.size();
        return internalRanks[last_rr_rank_Internal];
    }

    int getMostFilledBufferRank(bool feedback){
        int rankMax = -1;
        size_t sizeMax = 0;
        for(auto& [rank,ct] : ranks){
            if ((feedback && ct != ChannelType::FBK) || (!feedback && ct != ChannelType::FWD)) continue;
            auto& batchBB = buffers[rank];
            size_t sz = batchBB.second[batchBB.first]->size();
            if (sz > sizeMax) {
                rankMax = rank;
                sizeMax = sz;
            }
        }
        if (rankMax >= 0) return rankMax;

        do {
            last_rr_rank = (last_rr_rank + 1) % this->ranks.size();
        } while (this->ranks[last_rr_rank].second != (feedback ? ChannelType::FBK : ChannelType::FWD));
        return this->ranks[last_rr_rank].first; 
    }

public:
    TransportMPIS(std::pair<ChannelType, ff_endpoint> destEndpoint,
        std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF)
		: TransportTypeS(destEndpoint, gName, batchSize, messageOTF, internalMessageOTF) {}

    TransportMPIS( std::vector<std::pair<ChannelType,ff_endpoint>> destEndpoints_,
        std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF)
            : TransportTypeS(destEndpoints_, gName, batchSize, messageOTF, internalMessageOTF) {}


    virtual int send(message_t* task, bool external) {
        int rank;
        if (task->chid != -1)
            if(external) rank = dest2Rank[{task->chid, task->feedback ? ChannelType::FBK : ChannelType::FWD}]; 
            else rank = dest2Rank[{task->chid, ChannelType::INT}];
        else 
            rank = external ? getMostFilledBufferRank(task->feedback) : getMostFilledInternalBufferRank();
        
        auto& buffs = buffers[rank];
        if(external)
            assert(buffs.second.size() > 0);
        if (buffs.second[buffs.first]->push(task)) // the push triggered a flush, so we must go ion the next buffer
            buffs.first = (buffs.first + 1) % buffs.second.size(); // increment the used buffer of 1
    
        return 0;
    }

    virtual void finalize() {
        for(auto& [rank, ct] : ranks){
            auto& buffs = buffers[rank];
            buffs.second[buffs.first]->pushEOS();
        }

        for(auto& [rank, bb] : buffers)
            for(auto& b : bb.second) b->waitCompletion();
    }

    virtual int handshake(precomputedRT_t* rt) {
        for(auto& [ct, ep]: this->destEndpoints){
            int rank = ep.getRank();
            bool isInternal = ct == ChannelType::INT;
            haveInternal = haveInternal || isInternal;
            haveExternal = haveExternal || !isInternal;
            if (isInternal) 
                internalRanks.push_back(rank);
            else
                ranks.push_back({rank, ct});

            std::vector<batchBuffer*> appo;
            for(int i = 0; i < (isInternal ? internalMessageOTF : messageOTF); i++) appo.push_back(batchSize == 1 ? new directBatchBuffer(rank) : new batchBuffer(batchSize, rank));
            buffers.emplace(std::make_pair(rank, std::make_pair(0, std::move(appo))));
            
            if (handshakeHandler(rank, ct) < 0) return -1;

            for(auto& [k,v] : *rt){
                if (k.first != ep.groupName) continue;
                for(int dest : v)
                    dest2Rank[std::make_pair(dest, k.second)] = ep.getRank();
            }
           
        }

        this->destEndpoints.clear();
        return 0;
    }

    virtual void notify(ssize_t id, bool external) {
        if(external) 
            for (auto& [rank, _] : ranks){
                auto& buffs = buffers[rank];
                buffs.second[buffs.first]->push(new message_t(id, -2));
            }
        else {
            // send the EOS to all the internal connections
            for(const auto&rank : internalRanks){
                auto& buffs = buffers[rank];
                buffs.second[buffs.first]->pushEOS();
            }
        }
    }

protected:
    std::vector<int> internalRanks;
    std::vector<std::pair<int, ChannelType>> ranks;
    std::map<int, std::pair<int, std::vector<batchBuffer*>>> buffers;
    std::map<std::pair<int, ChannelType>, int> dest2Rank;
    int last_rr_rank = 0; //next destination to send for round robin policy
    int last_rr_rank_Internal = -1;

};
#endif


class TransportTCP: public TransportType {

protected:

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
        this->internalConnections += t == ChannelType::INT;
        std::cout << "Done handshake with " << groupName << "\n";
        return 0;
    }


    virtual int handleBatch(int sck){
        int requestSize;
        switch(readn(sck, reinterpret_cast<char*>(&requestSize), sizeof(requestSize))) {
		case -1: {			
			perror("readn");
			error("Something went wrong in receiving the number of tasks!\n");
			return -1;
		} break;
		case 0: return -1;
        }
		// always sending back the acknowledgement
        if (writen(sck, reinterpret_cast<char*>(&ACK), sizeof(ack_t)) < 0){
            if (errno != ECONNRESET && errno != EPIPE) {
                error("Error sending back ACK to the sender (errno=%d)\n",errno);
                return -1;
            }
        }
		ChannelType t = sck2ChannelType[sck];
        requestSize = ntohl(requestSize);
        for(int i = 0; i < requestSize; i++)
            if (handleRequest(sck, t)<0) return -1;
        
        return 0;
    }


    virtual int handleRequest(int sck, ChannelType t){
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
            case -1: error("Error reading from socket errno=%d\n",errno); // fatal error
            case  0: return -1; // connection close
        }

        // convert values to host byte order
        sender = ntohl(sender);
        chid   = ntohl(chid);
        sz     = be64toh(sz);

        if (sz > 0){
            receiver->received++;
            char* buff = new char [sz];
			assert(buff);
            if(readn(sck, buff, sz) < 0){
                error("Error reading from socket in handleRequest\n");
                delete [] buff;
                return -1;
            }
			message_t* out = new message_t(buff, sz, true);
			assert(out);
            out->feedback = t == ChannelType::FBK;
			out->sender = sender;
			out->chid   = chid;
            receiver->forward(out, sck2ChannelType[sck] == ChannelType::INT);

            return 0;
        }
        //logical EOS
        if (chid == -2){
            receiver->registerLogicalEOS(sender);
            return 0;
        }

        //pyshical EOS
        neos++;
        receiver->registerEOS(sck2ChannelType[sck] == ChannelType::INT);
        return -1;
    }

public:
    TransportTCP(ff_endpoint handshakeAddr, size_t input_channels)
		: TransportType(input_channels), handshakeAddr(handshakeAddr) {}


    virtual void init(ff_monode_t<message_t>* data) {
        receiver = (ff_dAreceiver*)data;

        this->_init();

        // intialize both sets (master, temp)
        FD_ZERO(&set);
        FD_ZERO(&tmpset);

        // add the listen socket to the master set
        FD_SET(this->listen_sck, &set);

        // hold the greater descriptor
        fdmax = this->listen_sck;
    }

    virtual int comm_listen() {
        if (neos < input_channels) {
        // while(neos < input_channels){
            // copy the master set to the temporary
            tmpset = set;
            struct timeval wait_time = {.tv_sec=0, .tv_usec=100000};

            switch(select(fdmax+1, &tmpset, NULL, NULL, &wait_time)){
                case -1: error("Error on selecting socket\n"); return -1;
                case  0: {return 1;}
            }

            for(int idx=0; idx <= fdmax; idx++){
	            if (FD_ISSET(idx, &tmpset)){
                    if (idx == this->listen_sck) {
                        int connfd = accept(this->listen_sck, (struct sockaddr*)NULL ,NULL);
                        if (connfd == -1){
                            error("Error accepting client\n");
                        } else {
                            FD_SET(connfd, &set);
                            if(connfd > fdmax) fdmax = connfd;
                            this->handshakeHandler(connfd);
                        }
                        return 1;
                    }
                    if (this->handleBatch(idx) < 0){
                        close(idx);
                        FD_CLR(idx, &set);

                        // update the maximum file descriptor
                        if (idx == fdmax)
                            for(int ii=(fdmax-1);ii>=0;--ii)
                                if (FD_ISSET(ii, &set)){
                                    fdmax = ii;
                                    break;
                                }
                    }
					
                }
            }
            return 1;
        }
        
        return 0;
    }

    virtual void finalize() {
        close(this->listen_sck);
    }


protected:
    ff_endpoint                 handshakeAddr;
    ff_dAreceiver*              receiver;

    std::map<int, ChannelType>  sck2ChannelType;
    int                         listen_sck;
    int                         last_receive_fd = -1;
    int                         fdmax;
    fd_set                      set, tmpset;
    size_t                      neos = 0;
    ack_t                       ACK;

};


class TransportTCPS: public TransportTypeS {

protected:
    std::map<int, unsigned int> socketsCounters;
    std::map<int, ff_batchBuffer> batchBuffers;
    std::map<std::pair<int, ChannelType>, int> dest2Socket;
    std::vector<int> internalSockets;
    int last_rr_socket = -1;
    int last_rr_socket_Internal = -1;

    int getMostFilledBufferSck(bool feedback){
        int sckMax = 0;
        int sizeMax = 0;
        for(auto& [sck, buffer] : batchBuffers){
            if ((feedback && buffer.ct != ChannelType::FBK) || (!feedback && buffer.ct != ChannelType::FWD)) continue; 
            if (buffer.size > sizeMax) sckMax = sck;
        }
    
        if (sckMax > 0) return sckMax;
        
        do {
        last_rr_socket = (last_rr_socket + 1) % this->socks.size();
        } while (batchBuffers[socks[last_rr_socket]].ct != (feedback ? ChannelType::FBK : ChannelType::FWD));
        return socks[last_rr_socket];
        
    }


    int getMostFilledInternalBufferSck(){
         int sckMax = 0;
        int sizeMax = 0;
        for(int sck : internalSockets){
            auto& b = batchBuffers[sck];
            if (b.size > sizeMax) {
                sckMax = sck;
                sizeMax = b.size;
            }
        }
        if (sckMax > 0) return sckMax;

        last_rr_socket_Internal = (last_rr_socket_Internal + 1) % this->internalSockets.size();
        return internalSockets[last_rr_socket_Internal];
    }


     int waitAckFrom(int sck){
        while (socketsCounters[sck] == 0){
            for(auto& [sck_, counter] : socketsCounters){
                int r; ack_t a;
                if ((r = recvnnb(sck_, reinterpret_cast<char*>(&a), sizeof(ack_t))) != sizeof(ack_t)){
                    if (errno == EWOULDBLOCK){
                        assert(r == -1);
                        continue;
                    }
                    perror("recvnnb ack");
                    return -1;
                } else {
                    counter++;
                }
                
            }
			
            if (socketsCounters[sck] == 0){
                tmpset = set;
                if (select(fdmax + 1, &tmpset, NULL, NULL, NULL) == -1){
                    perror("select");
                    return -1;
                }
            }
        }
        return 1;
    }



    virtual int handshakeHandler(const int sck, ChannelType ct) {
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
    TransportTCPS(std::pair<ChannelType, ff_endpoint> destEndpoint,
        std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF)
		: TransportTypeS(destEndpoint, gName, batchSize, messageOTF, internalMessageOTF) {}

    TransportTCPS( std::vector<std::pair<ChannelType,ff_endpoint>> destEndpoints_,
        std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF)
            : TransportTypeS(destEndpoints_, gName, batchSize, messageOTF, internalMessageOTF) {}


    virtual int send(message_t* task, bool external) {
        int sck;
        if (task->chid != -1)
            if(external) sck = dest2Socket[{task->chid, task->feedback ? ChannelType::FBK : ChannelType::FWD}]; 
            else sck = dest2Socket[{task->chid, ChannelType::INT}];
        else 
            sck = external ? getMostFilledBufferSck(task->feedback) : getMostFilledInternalBufferSck();
        
        if (batchBuffers[sck].push(task) == -1) {
			return -1;
		}

        return 0;
    }
    

    virtual int handshake(precomputedRT_t* rt) {
        FD_ZERO(&set);
        FD_ZERO(&tmpset);

        for (auto& [ct, ep] : this->destEndpoints)
        {
            std::cout << gName << " trying to connect to: " << ep.address << " gName: " << ep.groupName << "\n";
            int sck = tryConnect(ep);
            if (sck <= 0) {
                error("Error on connecting!\n");
                return -1;
            }
            bool isInternal = ct == ChannelType::INT;
            haveInternal = haveInternal || isInternal;
            haveExternal = haveExternal || !isInternal;
            if(isInternal) internalSockets.push_back(sck);
            else socks.push_back(sck);

            socketsCounters[sck] = isInternal ? internalMessageOTF : messageOTF;
            batchBuffers.emplace(std::piecewise_construct, std::forward_as_tuple(sck), std::forward_as_tuple(this->batchSize, ct, [this, sck](struct iovec* v, int size) -> bool {
                if (this->socketsCounters[sck] == 0 && this->waitAckFrom(sck) == -1){
                    error("Errore waiting ack from socket inside the callback\n");
                    return false;
                }

                if (writevn(sck, v, size) < 0){
                    error("Error sending the iovector inside the callback!\n");
                    return false;
                }

                this->socketsCounters[sck]--;

                return true;
            }));

            // compute the routing table!
            for(auto& [k,v] : *rt){
                if (k.first != ep.groupName) continue;
                for(int dest : v)
                    dest2Socket[std::make_pair(dest, k.second)] = sck;
            }
            
            if(handshakeHandler(sck, ct) < 0) {
				error("Error in handshake");
				return -1;
			}
            FD_SET(sck, &set);
            if(sck > fdmax) fdmax = sck;
        }

        // we can erase the list of endpoints
        this->destEndpoints.clear();
        return 0;
    }

    virtual void notify(ssize_t id, bool external) {
        if(external) {
            for (const auto& sck : socks)
                batchBuffers[sck].push(new message_t(id, -2));
        }
        else {
            for(const auto& sck : internalSockets) {
                if (batchBuffers[sck].sendEOS()<0) {
                    error("sending EOS to internal connections\n");
                }
                shutdown(sck, SHUT_WR);
            }
        }
    }

    virtual void finalize() {
        for(const auto& sck : socks) {
            if (batchBuffers[sck].sendEOS()<0) {
                error("sending EOS to external connections (ff_dsender)\n");
            }										 
            shutdown(sck, SHUT_WR);
        }

        // here we wait all acks from all connections
		size_t totalack  = internalSockets.size()*internalMessageOTF;
		totalack        += socks.size()*messageOTF;
		size_t currentack = 0;
		for(const auto& [_, counter] : socketsCounters)
			currentack += counter;

		ack_t a;
		while(currentack<totalack) {
			for(auto scit = socketsCounters.begin(); scit != socketsCounters.end();) {
				auto sck      = scit->first;
				auto& counter = scit->second;

				switch(recvnnb(sck, (char*)&a, sizeof(a))) {
				case 0:
				case -1: {
					if (errno == EWOULDBLOCK) {
						++scit;
						continue;
					}
					decltype(internalSockets)::iterator it;
					it = std::find(internalSockets.begin(), internalSockets.end(), sck);
					if (it != internalSockets.end())
						currentack += (internalMessageOTF-counter);
					else
						currentack += (messageOTF-counter);
					socketsCounters.erase(scit++);
				} break;
				default: {
					currentack++;
					counter++;
					++scit;					
				}					
				}
			}
		}
		for(const auto& [sck, _] : socketsCounters) close(sck);
    }

};


#endif //FFDCOMM
