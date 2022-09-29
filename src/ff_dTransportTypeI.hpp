/*
 * Interface for the transport-specific components. Two different interfaces are
 * provided:
 * 
 *      - ReceiverPlugin: implements the FastFlow communication protocol at the
 *                        receiver side.
 *      - SenderPlugin: implements the FastFlow communication protocol at the
 *                      sender side
 * 
 * The functionalities are provided by means of transport-specific calls.
 * Plugins will be orchestrated by the Manager object. They can be provided as a
 * vector to each Manager in order to specify which protocols the Manager is going
 * to accept. The functionalities implemented by the Plugins are mapped in the
 * Manager to realize the communication protocol of original FastFlow distributed
 * runtime.
 * 
 * Author:
 *      Federico Finocchio
 *
 * 
 * 
 */

#ifndef FF_DCOMP_I
#define FF_DCOMP_I

#include <map>
#include <ff/ff.hpp>
#include <ff/dff.hpp>
#include <ff/distributed/ff_network.hpp>

using namespace ff;
using precomputedRT_t = std::map<std::pair<std::string, ChannelType>, std::vector<int>>;

class ReceiverPlugin {
protected:

public:
    /**
     * @brief Implementation of receiver-side handshake procedure requested by
     * the FastFlow communication protocol.
     */
    virtual void init(ff_monode_t<message_t>*) = 0;

    /**
     * @brief Non-blocking implementation of receive functionalities. Used by the
     * manager to poll on this component for pending messages. Must return the
     * code specifying if a message is pending, or if connection has been closed.
     */
    virtual int wait_msg() = 0;

    /**
     * @brief Cleanup procedure for the current component. Must close all the
     * listening endpoints created during the handshake process.
     */
    virtual void finalize() = 0;

    virtual size_t getInternalConnections(){
        return this->internalConnections;
    }

protected:
    ReceiverPlugin(size_t input_channels)
            : input_channels(input_channels) {}

    size_t                  input_channels;
    bool                    internal;
    std::vector<int>        internalDestinations;
    std::set<std::string>   internalGroupsNames;
    size_t                  internalConnections = 0;
    std::map<int, bool>     isInternalConnection;
    size_t                  handshakes = 0;
};


class SenderPlugin {
protected:

public:
    /**
     * @brief Send implementation to ship data via the network to a ReceiverPlugin.
     * Used internally by the Manager for each task received from the Sender node.
     * 
     * @param task task to be shipped over the network
     * @param external whether the task comes from an external (true) or
     * internal (false) channel
     * @return int success (0) or failure (1)
     */
    virtual int send(message_t* task, bool external) = 0;

    /**
     * @brief Cleanup procedure for the current component. Must close all the
     * endpoints used for communications with a ReceiverPlugin.
     * 
     */
    virtual void finalize() = 0;

    /**
     * @brief Implements the sender-side handshake procedure for the FastFlow
     * communication protocol.
     * 
     * @param rt Routing table provided by the Manager. Represents the reachable
     * nodes from this component.
     * @return int return code, either 0 for success or -1 for failure
     */
    virtual int init(precomputedRT_t* rt) = 0;

    /**
     * @brief Notifies an external/internal node of reception of EOS message from
     * the Sender side.
     * 
     * @param id id for the recipient
     * @param external whether the EOS is external (true) or internal (false)
     */
    virtual void notify(ssize_t id, bool external) = 0;

    
    bool haveConnType(bool external) {
        return external ? haveExternal : haveInternal;
    }

protected:
    SenderPlugin(std::pair<ChannelType, ff_endpoint> destEndpoint,
        std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF)
		: gName(gName), batchSize(batchSize), messageOTF(messageOTF),
        internalMessageOTF(internalMessageOTF) {
        this->destEndpoints.push_back(std::move(destEndpoint));
    }

    SenderPlugin( std::vector<std::pair<ChannelType,ff_endpoint>> destEndpoints_,
        std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF)
            : destEndpoints(std::move(destEndpoints_)), gName(gName),
            batchSize(batchSize), messageOTF(messageOTF),
            internalMessageOTF(internalMessageOTF) {}


    std::vector<std::pair<ChannelType, ff_endpoint>>    destEndpoints;
    std::string                                         gName;
    int                                                 batchSize;
    int                                                 messageOTF;
    int                                                 internalMessageOTF;
    fd_set                                              set, tmpset;
    int                                                 fdmax = -1;
    std::vector<int>                                    socks; 
    bool                                                haveExternal=false, haveInternal=false;
};

#endif
