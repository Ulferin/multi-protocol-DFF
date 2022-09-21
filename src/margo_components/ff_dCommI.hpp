/* This is a generic interface used to provide protocol-specific functionalities
to remotely connected FastFlow's nodes. It must be extended in order to implement
the barely necessary functions to receive and ship data in the network. */

#ifndef FF_DCOMM_I
#define FF_DCOMM_I

#include <map>
#include <ff/ff.hpp>
#include <ff/dff.hpp>
#include <ff/distributed/ff_network.hpp>
#include "../ff_dCompI.hpp"

using namespace ff;

class ff_dCommunicatorRPCS: public ff_dCompS {
protected:

    


public:

protected:
    ff_dCommunicatorRPCS(std::pair<ChannelType, ff_endpoint> destEndpoint,
        std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF):
            ff_dCompS(destEndpoint, gName, batchSize, messageOTF, internalMessageOTF){
    }


    ff_dCommunicatorRPCS(std::vector<std::pair<ChannelType,ff_endpoint>> destEndpoints_,
        std::string gName = "",
        int batchSize = DEFAULT_BATCH_SIZE, int messageOTF = DEFAULT_MESSAGE_OTF,
        int internalMessageOTF = DEFAULT_INTERNALMSG_OTF):
            ff_dCompS(destEndpoints_, gName, batchSize, messageOTF, internalMessageOTF){}


    
};

#endif
