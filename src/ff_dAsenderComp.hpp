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
#ifndef FF_DASENDER_COMP
#define FF_DASENDER_COMP

#include <iostream>
#include <vector>
#include <thread>

#include <ff/ff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_dgroups.hpp>
// #include "ff_dCompI.hpp"
#include "ff_dCommMasterI.hpp"

using namespace ff;

class ff_dAsender: public ff_minode_t<message_t> {
public:
    ff_dAsender(ff_dSenderMasterI* communicator, int coreid = -1, int busy = 1):
            communicator(communicator), coreid(coreid), busy(busy) {
        
        // this->communicator->init();
    }


    int svc_init() {
		if (coreid!=-1)
			ff_mapThreadToCpu(coreid);
		
        if (communicator->init() == -1) {
            error("Handhsake error.\n");
            return -1;
        }

        return 0;
    }

    void svc_end() {
        printf("[SENDER] Received: %d\n", received);
        // communicator->finalize();
    }

    message_t *svc(message_t* task) {
        received++;
        if(communicator->send(task, true) == -1)
            return EOS;

        return this->GO_ON;
    }

    void eosnotify(ssize_t id) {

        communicator->notify(id, true);

        if (++neos >= this->get_num_inchannels()) {
            communicator->finalize();
        }
    }

protected:

    // From ff_dsender
    ff_dSenderMasterI*                  communicator;
    size_t                              neos=0;
    int                                 nextExternal = 0, nextInternal = 0;
    int                                 coreid;
    int                                 busy;

    std::map<int, int>::const_iterator  rr_iterator;
    std::map<int, int>                  internalDest2Socket;
    int                                 externalDests = 0, internalDests = 0;

    int received = 0;
};

class ff_dAsenderH: public ff_dAsender {

protected:
bool squareBoxEOS = false;

public:

    ff_dAsenderH(ff_dSenderMasterI* communicator, int coreid = -1, int busy = 1):
            ff_dAsender(communicator, coreid, busy) {}

    message_t* svc(message_t* task) {
        if (this->get_channel_id() == (ssize_t)(this->get_num_inchannels() - 1)){
            received++;
            if(communicator->send(task, false) == -1)
                return EOS;
            return this->GO_ON;
        }
        
        return ff_dAsender::svc(task);
    }

    void eosnotify(ssize_t id) {
        if (id == (ssize_t)(this->get_num_inchannels() - 1)){
            if (squareBoxEOS) return;
            squareBoxEOS = true;
            communicator->notify(id, false);
        }
        if (++neos >= this->get_num_inchannels())
            communicator->finalize();
    }

};

#endif