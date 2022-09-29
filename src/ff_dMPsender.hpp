#ifndef FF_DMPSENDER_COMP
#define FF_DMPSENDER_COMP

#include <iostream>
#include <vector>
#include <thread>

#include <ff/ff.hpp>
#include <ff/distributed/ff_network.hpp>
#include <ff/distributed/ff_dgroups.hpp>
#include <ff_dManagerI.hpp>

using namespace ff;

class ff_dMPsender: public ff_minode_t<message_t> {
public:
    ff_dMPsender(SenderManagerI* communicator, int coreid = -1, int busy = 1):
            communicator(communicator), coreid(coreid), busy(busy) {}


    int svc_init() {
		if (coreid!=-1)
			ff_mapThreadToCpu(coreid);
		
        if (communicator->init() == -1) {
            error("Handhsake error.\n");
            return -1;
        }

        return 0;
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
    SenderManagerI*                     communicator;
    size_t                              neos=0;
    int                                 nextExternal = 0, nextInternal = 0;
    int                                 coreid;
    int                                 busy;

    std::map<int, int>::const_iterator  rr_iterator;
    std::map<int, int>                  internalDest2Socket;
    int                                 externalDests = 0, internalDests = 0;

    int                                 received = 0;
};


class ff_dMPsenderH: public ff_dMPsender {

protected:
bool squareBoxEOS = false;

public:

    ff_dMPsenderH(SenderManagerI* communicator, int coreid = -1, int busy = 1):
            ff_dMPsender(communicator, coreid, busy) {}

    message_t* svc(message_t* task) {
        if (this->get_channel_id() == (ssize_t)(this->get_num_inchannels() - 1)){
            received++;
            if(communicator->send(task, false) == -1)
                return EOS;
            return this->GO_ON;
        }
        
        return ff_dMPsender::svc(task);
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