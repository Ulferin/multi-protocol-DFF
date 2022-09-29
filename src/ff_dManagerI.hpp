/*
 * Interface for the Manager components. Two different interfaces are
 * provided:
 * 
 *      - ReceiverManagerI: orchestrates ReceiverPlugins to realize the FastFlow
 *                  receiver-side communication protocol using multiple transports.
 * 
 *      - SenderManagerI: orchestrates SenderPlugins to realize the FastFlow
 *                  receiver-side communication protocol using multiple transports.
 * 
 * Each Manager object has a vector of Plugin objects which orchestrate by means
 * of a generic interface in order to receive and send data between dgroups using
 * multiple protocols.
 * 
 * Author:
 *      Federico Finocchio
 *
 * 
 * 
 */

#ifndef FF_DCOMM_MASTER_I
#define FF_DCOMM_MASTER_I

#include <ff/ff.hpp>
#include <ff/dff.hpp>
#include <ff/distributed/ff_network.hpp>

class ReceiverManagerI {

public:
    /**
     * @brief Registers the receiver node inside each of the Plugins. The receiver
     * is then retrieved inside the plugin to send data to local nodes in the dgroup.
     * Performs the handshake for all the components in this Manager object.
     * 
     * @param receiver receiver node for this dgroup
     */
    virtual void init(ff_monode_t<message_t>* receiver) = 0;

    /**
     * @brief Polls each of the components to check if data has been received.
     * Represents the main progress loop for the multi-protocol communication
     * between distributed groups.
     * 
     * @return int error code
     */
    virtual int wait_components() = 0;

    /**
     * @brief Cleanup all the internal components
     * 
     */
    virtual void finalize() = 0;

    // Getters
    virtual int getChannelID(int chid) = 0;
    virtual size_t getInternalConnections() = 0;
};

class SenderManagerI {

public:
    /**
     * @brief Performs the handshake on all the components inside this Manager.
     * 
     * @return int error code
     */
    virtual int init() = 0;

    /**
     * @brief Sends a message to the paired Receiver. Uses the internal routing
     * table in order to select the correct component to ship the data, also
     * based on the external/internal type of message.
     * 
     * @param task task to be sent
     * @param external whether if the task is related to an external/internal
     * communication channel
     * @return int return code
     */
    virtual int send(message_t* task, bool external) = 0;

    /**
     * @brief Notifies all the components about the reception of an EOS message
     * 
     * @param id id of the sender
     * @param external whether it is an external or internal EOS
     */
    virtual void notify(ssize_t id, bool external) = 0;

    /**
     * @brief Cleanup all the internal components in this Manager
     * 
     */
    virtual void finalize() = 0;
};


#endif