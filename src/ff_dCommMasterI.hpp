#ifndef FF_DCOMM_MASTER_I
#define FF_DCOMM_MASTER_I

// #include "ff_dAreceiverComp.hpp"
#include <ff/ff.hpp>
#include <ff/dff.hpp>
#include <ff/distributed/ff_network.hpp>

class ff_dReceiverMasterI {

public:
    virtual void init(ff_monode_t<message_t>* receiver) = 0;
    virtual int wait_components() = 0;
    virtual void finalize() = 0;
    virtual int getChannelID(int chid) = 0;
    virtual size_t getInternalConnections() = 0;
};

class ff_dSenderMasterI {

public:
    virtual int init() = 0;
    virtual int send(message_t* task, bool external) = 0;
    virtual void notify(ssize_t id, bool external) = 0;
    virtual void finalize() = 0;
};


#endif