/*
 * Implementation of the Manager interface.
 * 
 * Author:
 *      Federico Finocchio
 * 
 */

#ifndef FF_DCOMM_MASTER
#define FF_DCOMM_MASTER

#include <vector>
#include <map>
#include <ff_dMPreceiver.hpp>
#include <ff_dTransportTypeI.hpp>
#include <ff_dManagerI.hpp>

#define MASTER_SENDER_MODE 0
#define MASTER_RECEIVER_MODE 1


class ReceiverManager: public ReceiverManagerI {

public:
    ReceiverManager(std::vector<ReceiverPlugin*> components,
        std::map<int, int> routingTable = {std::make_pair(0,0)})
          :components(std::move(components)), routingTable(routingTable){}

    void init(ff_monode_t<message_t>* receiver) {
        // First initialize receivers in order to allow them to handle handshakes        
        for (auto &component : components) {
            component->init(receiver);
        }      
    }


    int wait_components() {
        std::vector<std::pair<bool, ReceiverPlugin*>> listeningComponents;
        for (auto &&component : components)
            listeningComponents.push_back(std::make_pair(false, component));
        
        while(!end) {
            end = true;
            for (auto &[fin, component] : listeningComponents) {
                end = end && fin;

                if(!fin) {
                    int res;
                    if((res = component->wait_msg()) == -1) {
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

protected:
    std::vector<ReceiverPlugin*>        components;
    std::map<int, int>                  routingTable;
    bool                                end = false;


};


class SenderManager : public SenderManagerI {

protected:
    SenderPlugin* getNextComponent(bool external) {
        SenderPlugin* component;
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
    SenderManager(
        std::vector<std::pair<std::set<std::string>, SenderPlugin*>> components,
        precomputedRT_t* rt)
        : components(std::move(components)), rt(rt) {}


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
            if(rtComp.second->init(rtComp.first) == -1) {
                error("Handhsake error.\n");
                return -1;
            }
        }

        it = componentsMap.begin();

        return 0;
    }

    int send(message_t* task, bool external) {
        received++;
        SenderPlugin* component;
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

protected:
    std::vector<std::pair<std::set<std::string>, SenderPlugin*>>    components;
    precomputedRT_t*                                                rt;
    std::map<std::pair<int, ff::ChannelType>, SenderPlugin*>        componentsMap;
    int                                                             next_component = 0;
    int                                                             received = 0;
    std::map<std::pair<int, ff::ChannelType>, SenderPlugin*>::iterator it;
    std::vector<std::pair<precomputedRT_t*, SenderPlugin*>>         routedComponents;


};
#endif //FF_DCOMM_MASTER