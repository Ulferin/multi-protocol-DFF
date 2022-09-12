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

#include "ff_dCommI.hpp"



using namespace ff;

class ff_dAreceiver: public ff_monode_t<message_t> {


public:

    ff_dAreceiver(ff_dCommunicator* communicator,
        size_t input_channels, int coreid = -1, int busy = 0, int expected=-1):
            communicator(communicator), input_channels(input_channels),
            coreid(coreid), busy(busy), expected(expected)
    {
        this->communicator->init(this, input_channels);
    }


    int svc_init() {
  		if (coreid!=-1)
			ff_mapThreadToCpu(coreid);
        
        return 0;
    }


    message_t *svc(message_t* task) { 
        if(communicator->comm_listen() == -1) {
            error("Listening for messages\n");
        }
        return this->EOS;
    }


    virtual void forward(message_t* task, bool isInternal){
        if (isInternal) ff_send_out_to(task, this->get_num_outchannels()-1);
        else ff_send_out_to(task, communicator->getChannelID(task->chid)); // assume the routing table is consistent WARNING!!!

        if(expected < received && neos == input_channels) {
            printf("I'm in the forward and I should terminate now!\n");
            for(size_t i = 0; i < get_num_outchannels()-1; i++) ff_send_out_to(this->EOS, i);
            communicator->finalize();
        }
    }


    virtual void registerEOS(bool isInternal) {

        if(!isInternal) {
            if (++this->externalNEos == (this->input_channels-communicator->getInternalConnections()))
                for(size_t i = 0; i < get_num_outchannels()-1; i++) ff_send_out_to(this->EOS, i);
        } else {
            if (++this->internalNEos == communicator->getInternalConnections())
                ff_send_out_to(this->EOS, get_num_outchannels()-1);
        }

        if(++neos == input_channels) {
            printf("Terminating || Expected: %d - Received: %d\n", expected, received);
            printf("Terminating but I received only %d\n", this->received);
            communicator->finalize(); 
        }  
    }

    void svc_end() {
        printf("[RECEIVER] Received: %d\n", received);
    }

protected:
    ff_dCommunicator*   communicator;
    size_t              input_channels;
    int                 coreid;
    int                 busy;

    size_t              neos = 0;
    size_t              internalNEos = 0, externalNEos = 0;

public:
    int received = 0;
    int expected = -1;
};

#endif