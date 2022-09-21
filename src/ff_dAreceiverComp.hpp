#ifndef FF_DARECEIVER_COMP
#define FF_DARECEIVER_COMP

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

#include <ff_dManagerI.hpp>



using namespace ff;

class ff_dAreceiver: public ff_monode_t<message_t> {


public:

    ff_dAreceiver(ReceiverManagerI* communicator, size_t input_channels,
        int coreid = -1, int busy = 0, int expected=-1):
            communicator(communicator), input_channels(input_channels),
            coreid(coreid), busy(busy), expected(expected) {}


    int svc_init() {
  		if (coreid!=-1)
			ff_mapThreadToCpu(coreid);
        
        communicator->init(this);
        return 0;
    }


    message_t *svc(message_t* task) { 
        if(communicator->wait_components() == -1) {
            error("Listening for messages\n");
        }
        return this->EOS;
    }


    virtual void forward(message_t* task, bool isInternal){
        if (task->chid == -1) ff_send_out(task);
        else ff_send_out_to(task, communicator->getChannelID(task->chid)); // assume the routing table is consistent WARNING!!!
    }

    virtual void registerLogicalEOS(int sender){
        for(size_t i = 0; i < this->get_num_outchannels(); i++)
            ff_send_out_to(new message_t(sender, i), i);
    }

    virtual void registerEOS(bool isInternal) {
        if(++neos == input_channels) {
            communicator->finalize();
        }
    }

    void svc_end() {
        printf("[RECEIVER] Received: %d\n", received);
    }

protected:
    ReceiverManagerI*   communicator;
    size_t              input_channels;
    int                 coreid;
    int                 busy;

    size_t              neos = 0;
    size_t              internalNEos = 0, externalNEos = 0;

public:
    int received = 0;
    int expected = -1;
};

class ff_dAreceiverH : public ff_dAreceiver {
protected:
    size_t internalNEos = 0, externalNEos = 0;
    int next_rr_destination = 0;

public:
    ff_dAreceiverH(ReceiverManagerI* communicator, size_t input_channels,
        int coreid = -1, int busy = 0, int expected=-1)
		: ff_dAreceiver(communicator, input_channels, coreid, busy, expected)  {}

    virtual void registerLogicalEOS(int sender){
        for(size_t i = 0; i < this->get_num_outchannels()-1; i++)
            ff_send_out_to(new message_t(sender, i), i);
    }

    virtual void registerEOS(bool isInternal){
        // neos++;
        if(!isInternal) {
            if (++this->externalNEos == (this->input_channels-communicator->getInternalConnections())) {
                for(size_t i = 0; i < get_num_outchannels()-1; i++) ff_send_out_to(this->EOS, i);
            }
        } else {
            if (++this->internalNEos == communicator->getInternalConnections()) {
                ff_send_out_to(this->EOS, get_num_outchannels()-1);
            }
        }

        ff_dAreceiver::registerEOS(isInternal);
    }

    virtual void forward(message_t* task, bool isInternal){
        if (isInternal)
            ff_send_out_to(task, this->get_num_outchannels()-1);
        else if (task->chid != -1)
            ff_send_out_to(task, communicator->getChannelID(task->chid));
        else {
            ff_send_out_to(task, next_rr_destination);
            next_rr_destination = ((next_rr_destination + 1) % (this->get_num_outchannels()-1));
        }
    }

};

#endif