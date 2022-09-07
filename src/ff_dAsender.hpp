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
#include "ff_dCommI.hpp"

using namespace ff;

class ff_dAsender: public ff_minode_t<message_t> {
public:
    ff_dAsender(ff_dCommunicatorS* communicator, int coreid = -1, int busy = 1):
            communicator(communicator), coreid(coreid), busy(busy) {
        
        this->communicator->init();
    }


    int svc_init() {
		if (coreid!=-1)
			ff_mapThreadToCpu(coreid);
		
        if (communicator->handshake() == -1) {
            error("Handhsake error.\n");
            return -1;
        }

        this->externalDests = communicator->getExternalCount();
        this->internalDests = communicator->getInternalCount();
        this->internalDest2Socket = communicator->getInternalMap();
        rr_iterator = internalDest2Socket.cbegin();

        return 0;
    }

    void svc_end() {
        printf("[SENDER] Received: %d\n", received);
        communicator->finalize();
    }

    message_t *svc(message_t* task) {
        received++;
        // Conditionally retrieve endpoint information and RPC id based on
        // internal/external chid.
        if (internalDests > 0
            && (this->get_channel_id() == (ssize_t)(this->get_num_inchannels() - 1))){

            // pick destination from the list of internal connections!
            if (task->chid == -1){ // roundrobin over the destinations
                task->chid = rr_iterator->first;
                if (++rr_iterator == internalDest2Socket.cend()) rr_iterator = internalDest2Socket.cbegin();
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

        if (internalDests > 0
            && id == (ssize_t)(this->get_num_inchannels() - 1)){
            std::cout << "Received EOS message from RBox!\n";
            message_t E_O_S(0,0);
            // send the EOS to all the internal connections
            communicator->send(&E_O_S, false);                           
        }

        if (++neos >= this->get_num_inchannels()) {
            message_t E_O_S(0,0);
            printf("[SENDER] Sending an external EOS\n");
            communicator->send(&E_O_S, true);
        }
    }

protected:

    // From ff_dsender
    ff_dCommunicatorS*                  communicator;
    size_t                              neos=0;
    int                                 nextExternal = 0, nextInternal = 0;
    int                                 coreid;
    int                                 busy;

    std::map<int, int>::const_iterator  rr_iterator;
    std::map<int, int>                  internalDest2Socket;
    int                                 externalDests = 0, internalDests = 0;

    int received = 0;
};

#endif