#ifndef FF_DCOMM_MASTER
#define FF_DCOMM_MASTER

#include <vector>
#include <map>
#include "ff_dAreceiverComp.hpp"
#include "ff_dCompI.hpp"
#include "ff_dCommMasterI.hpp"

#include "margo_components/ff_dCommI.hpp"

#define MASTER_SENDER_MODE 0
#define MASTER_RECEIVER_MODE 1


class ff_dReceiverMaster: public ff_dReceiverMasterI {

protected:
    std::vector<ff_dComp*>      components;
    std::map<int, int>          routingTable;
    bool                        end = false;

public:
    ff_dReceiverMaster(std::vector<ff_dComp*> components,
        std::map<int, int> routingTable = {std::make_pair(0,0)})
          :components(std::move(components)), routingTable(routingTable){
        
        for (auto &&component : this->components) {
            component->boot_component();
        }
        
    }

    void init(ff_monode_t<message_t>* receiver) {
        // First initialize receivers in order to allow them to handle handshakes        
        for (auto &component : components) {
            component->init(receiver);
        }      
    }


    int wait_components() {
        std::vector<std::pair<bool, ff_dComp*>> listeningComponents;
        for (auto &&component : components)
            listeningComponents.push_back(std::make_pair(false, component));
        
        while(!end) {
            end = true;
            for (auto &[fin, component] : listeningComponents) {
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
        for(auto &component : components) {
            component->finalize();
        }
    }

    int getChannelID(int chid) {
        return this->routingTable[chid];
    }

    size_t getInternalConnections() {
        size_t internalConn = 0;
        for(auto &component : components) {
            internalConn += component->getInternalConnections();
        }

        return internalConn;
    }


};


class ff_dSenderMaster : public ff_dSenderMasterI {

protected:
    std::vector<std::pair<std::set<std::string>, ff_dCompS*>> components;
    precomputedRT_t* rt;
    std::map<std::pair<int, ff::ChannelType>, ff_dCompS*>        componentsMap;
    int next_component = 0;
    int received = 0;
    std::map<std::pair<int, ff::ChannelType>, ff_dCompS*>::iterator it;
    std::vector<std::pair<precomputedRT_t*, ff_dCompS*>> routedComponents;

    ff_dCompS* getNextComponent(bool external) {
        ff_dCompS* component;
        // if(it == componentsMap.end()) it = componentsMap.begin();
        it = componentsMap.begin();
        while(it != componentsMap.end()) {
            component = it->second;
            it++;
            if(component->haveConnType(external)) break;
        }
        return component;
    }

public:
    ff_dSenderMaster(
        std::vector<std::pair<std::set<std::string>, ff_dCompS*>> components,
        precomputedRT_t* rt)
        : components(std::move(components)), rt(rt) {
        for (auto &&[_, component] : this->components) {
            component->boot_component();
        }
    }


    int init() {

        // Build the association between task->chid and components.
        for(auto& [groups, component] : components) {
            precomputedRT_t* tempRout = new precomputedRT_t;
            for(auto& [k,v] : *rt){
                if (!groups.contains(k.first)) continue;
                (*tempRout)[k] = v;
                for(int dest : v)
                    componentsMap[std::make_pair(dest, k.second)] = component;
            }
            routedComponents.push_back(std::make_pair(tempRout, component));
        }

        for(auto& rtComp : routedComponents) {
            if(rtComp.second->handshake(rtComp.first) == -1) {
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
        if(task->chid == -1) {
            component = getNextComponent(external);
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