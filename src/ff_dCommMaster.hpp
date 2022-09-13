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


class ff_dSenderMaster : public ff_dSenderMasterI {

protected:
    std::vector<std::pair<std::set<std::string>, ff_dCompS*>> components;
    std::map<std::pair<std::string, ChannelType>, std::vector<int>> *rt;
    std::map<std::pair<int, ff::ChannelType>, ff_dCompS*>        componentsMap;
    int next_component = 0;
    int received = 0;
    std::map<std::pair<int, ff::ChannelType>, ff_dCompS*>::iterator it;
public:
    ff_dSenderMaster(
        std::vector<std::pair<std::set<std::string>, ff_dCompS*>> components,
        std::map<std::pair<std::string, ChannelType>, std::vector<int>> *rt)
        : components(std::move(components)), rt(rt) {}


    int init() {

        // Build the association between task->chid and components.
        for(auto& [groups, component] : components) {
            for(auto& [k,v] : *rt){
                if (!groups.contains(k.first)) continue;
                std::cout << componentsMap.size() << "\n";
                for(int dest : v)
                    componentsMap[std::make_pair(dest, k.second)] = component;
            }
        }
        //FIXME: qui costruire la RT da passare all'handshake dei singoli components
        for(auto& [groups, component] : components) {
            if(component->handshake() == -1) {
                error("Handhsake error.\n");
                return -1;
            }
        }

        it = componentsMap.begin();

        return 0;
    }

    int send(message_t* task, bool external) {
        received++;
        ff_dCompS* component;
        ff::ChannelType cht = external ? (task->feedback ? ChannelType::FBK : ChannelType::FWD) : ff::ChannelType::INT;
        if(it == componentsMap.end()) it = componentsMap.begin();
        if(task->chid == -1) {
            component = it->second;
            it++;
        }
        else component = componentsMap[{task->chid, cht}];
        if(component->send(task, external) == -1)
            return -1;
        
        else return 0;
    }

    void notify(ssize_t id, bool external) {
        for(auto& [groups, component] : components) {
            component->notify(id, external);
        }
    }

    void finalize() {
        printf("[MASTER-SENDER] Received: %d\n", received);
        for(auto& [groups, component] : components) {
            component->finalize();
        }
    }


};
#endif //FF_DCOMM_MASTER