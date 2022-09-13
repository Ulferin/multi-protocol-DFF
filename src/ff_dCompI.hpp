/* This is a generic interface used to provide protocol-specific functionalities
to remotely connected FastFlow's nodes. It must be extended in order to implement
the barely necessary functions to receive and ship data in the network. */

#ifndef FF_DCOMP_I
#define FF_DCOMP_I

#include <map>
#include <ff/ff.hpp>
#include <ff/dff.hpp>
#include <ff/distributed/ff_network.hpp>

using namespace ff;
using precomputedRT_t = std::map<std::pair<std::string, ChannelType>, std::vector<int>>;

class ff_dComp {
protected:

public:
    // FIXME: qui in realtà vorremmo una callback da chiamare in qualche modo, non l'intero oggetto
    virtual void init(ff_monode_t<message_t>*) = 0;
    virtual int comm_listen() = 0;
    virtual void finalize() = 0;
    
    int getChannelID(int chid) {
        return this->routingTable[chid];
    }

    virtual size_t getInternalConnections(){
        return this->internalConnections;
    }

protected:
    //FIXME: l'input_channels nel caso di gestione di più receiver è il numero di
    //      connessioni in entrata su quel componente
    ff_dComp(size_t input_channels,
        std::map<int, int> routingTable = {std::make_pair(0,0)})
            : input_channels(input_channels), routingTable(routingTable) {}

    size_t                  input_channels;
    std::map<int, int>      routingTable;
    bool                    internal;
    std::vector<int>        internalDestinations;
    std::set<std::string>   internalGroupsNames;
    size_t                  internalConnections = 0;
    std::map<int, bool>     isInternalConnection;

    
    size_t                  handshakes = 0;
};


class ff_dCompS {
protected:

public:
    virtual void init() = 0;
    virtual int send(message_t* task, bool external) = 0;
    virtual void finalize() = 0;

    virtual int handshake() = 0;

    virtual void notify(ssize_t id, bool external) = 0;

protected:
    ff_dCompS(std::pair<ChannelType, ff_endpoint> destEndpoint,
        precomputedRT_t* rt, std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF)
		: rt(rt), gName(gName), batchSize(batchSize), messageOTF(messageOTF),internalMessageOTF(internalMessageOTF) {
        this->destEndpoints.push_back(std::move(destEndpoint));
    }

    ff_dCompS( std::vector<std::pair<ChannelType,ff_endpoint>> destEndpoints_,
        precomputedRT_t* rt, std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF)
            : rt(rt), destEndpoints(std::move(destEndpoints_)), gName(gName),
            batchSize(batchSize), messageOTF(messageOTF), internalMessageOTF(internalMessageOTF) {}


    precomputedRT_t*                                    rt;
    std::vector<std::pair<ChannelType, ff_endpoint>>    destEndpoints;
    std::string                                         gName;
    int                                                 batchSize;
    int                                                 messageOTF;
    int                                                 internalMessageOTF;
    std::set<std::string>                               internalGroups;
    fd_set                                              set, tmpset;
    int                                                 fdmax = -1;
    std::vector<int>                                    socks; 
};

#endif
