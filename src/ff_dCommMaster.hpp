#ifndef FF_DCOMM_MASTER
#define FF_DCOMM_MASTER

#include <vector>
#include <map>
#include "ff_dAreceiverComp.hpp"
#include "ff_dCompI.hpp"
#include "ff_dCommMasterI.hpp"

#define MASTER_SENDER_MODE 0
#define MASTER_RECEIVER_MODE 1


class ff_dReceiverMaster: public ff_dReceiverMasterI {

protected:
    std::vector<std::pair<bool, ff_dComp*>>      components;
    std::map<int, int>                          routingTable;
    bool                                        end = false;

public:
    ff_dReceiverMaster(std::vector<std::pair<bool, ff_dComp*>> components,
        std::map<int, int> routingTable = {std::make_pair(0,0)})
          :components(std::move(components)), routingTable(routingTable){}

    void init(ff_monode_t<message_t>* receiver) {
        // First initialize receivers in order to allow them to handle handshakes        
        for (auto &[fin, component] : components) {
            component->init(receiver);
        }      
    }

    /**
     * NOTE: here we should have two things: the first is a cycle over all the
     *   components listen call and the check for the termination condition,
     *   which should be the conditional composition of all the conditions
     *   in the components. In this way, every time a component closes the
     *   connection because the EOS are received from it, we remove from the
     *   "active" receivers and we update the general condition. 
    */
    int wait_components() {
        while(!end) {
            end = true;
            for (auto &[fin, component] : components) {
                end = end && fin;

                if(!fin) {
                    int res;
                    if((res = component->comm_listen()) == -1) {
                        error("Listening for messages\n");
                        return -1;     // In case one listener crashes, we close everything
                    }
                    if(res == 0) fin = true;
                }
                else continue;
            }
        }
        printf("[MASTER] Terminating\n");
        
        return 0;
    }

    void finalize() {
        for(auto &[fin, component] : components) {
            component->finalize();
        }
    }

    int getChannelID(int chid) {
        return this->routingTable[chid];
    }

    size_t getInternalConnections() {
        size_t internalConn = 0;
        for(auto &[fin, component] : components) {
            internalConn += component->getInternalConnections();
        }

        return internalConn;
    }


};


#endif //FF_DCOMM_MASTER