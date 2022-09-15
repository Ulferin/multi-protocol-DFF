/*
 *           
 *           |-> Forwarder1 ->|    |-> Sink1 ->|  
 *  Source ->|                | -> |           | -> StringPrinter
 *           |-> Forwarder2 ->|    |-> Sink2 ->|
 *          
 *
 * 
 *  G0: Source
 *  G1: Forwarer1, Sink1
 *  G2: Forwarder2, Sink2
 *  G3: StringPrinter
 *
 */

#include <iostream>
#include <ff/dff.hpp>
#include <ff/distributed/ff_dadapters.hpp>
#include <mutex>
#include <map>

#include "ff_dCommComp.hpp"
#include "ff_dAreceiverComp.hpp"
#include "ff_dAsenderComp.hpp"
#include "ff_dCommMaster.hpp"

using namespace ff;
std::mutex mtx;

struct RealSource : ff_monode_t<std::string>{
    int ntask;
    RealSource(int ntask) : ntask(ntask) {}

    std::string* svc(std::string*){
        for(int i = 0; i < ntask; i++)
            ff_send_out(new std::string("Trigger string!"));
        return EOS;
    }
};

struct Source : ff_monode_t<std::string>{
    int numWorker, generatorID;
    Source(int numWorker, int generatorID) : numWorker(numWorker), generatorID(generatorID) {}

    std::string* svc(std::string* in){
        delete in;
        std::cout << "Source starting generating tasks!" << std::endl;
        for(int i = 0; i < numWorker; i++)
			ff_send_out_to(new std::string("Task" + std::to_string(i) + " generated from " + std::to_string(generatorID) + " for " + std::to_string(i)), i);
        
        return GO_ON;
    }
};


struct Sink : ff_minode_t<std::string>{
    int sinkID;
    Sink(int id): sinkID(id) {}
    std::string* svc(std::string* in){
        printf("[Sink] Received task %s\n", std::to_string(get_channel_id()).c_str());
        std::string* output = new std::string(*in + " received by Sink " + std::to_string(sinkID) + " from " +  std::to_string(get_channel_id()));
        delete in;
        return output;
    }
};

struct StringPrinter : ff_node_t<std::string>{
    std::string* svc(std::string* in){
        const std::lock_guard<std::mutex> lock(mtx);
        std::cout << "Received something! Addr:" << in << "\n";
#if 1
        try {
            std::cout << *in << std::endl;
            delete in;
        } catch (const std::exception& ex){
            std::cerr << ex.what();
        }
#endif		
        return this->GO_ON;
    }
};


struct ForwarderNode : ff_node { 
        ForwarderNode(std::function<bool(void*, dataBuffer&)> f,
					  std::function<void(void*)> d) {			
            this->serializeF = f;
			this->freetaskF  = d;
        }
        ForwarderNode(std::function<void*(dataBuffer&,bool&)> f,
					  std::function<void*(char*,size_t)> a) {
			this->alloctaskF   = a;
            this->deserializeF = f;
        }
        void* svc(void* input){ return input;}
    };


int main(int argc, char*argv[]){

    if (argc != 3){
        std::cerr << "Execute with the index of process!" << std::endl;
        return 1;
    }

    int ntask = atoi(argv[2]);

    ff_endpoint g1("127.0.0.1", 8001);
    g1.groupName = "G1";

    ff_endpoint g1_2("127.0.0.1", 8004);
    g1_2.groupName = "G1";

    ff_endpoint g2("127.0.0.1", 8002);
    g2.groupName = "G2";

    ff_endpoint g2_2("127.0.0.1", 8005);
    g2_2.groupName = "G2";

    ff_endpoint g3("127.0.0.1", 8003);
    g3.groupName = "G3";

    ff_farm gFarm;
    ff_a2a a2a;
    std::map<std::pair<std::string, ChannelType>, std::vector<int>> rt;
    if (atoi(argv[1]) == 0){
        rt[std::make_pair(g1.groupName, ChannelType::FWD)] = std::vector({0});
        rt[std::make_pair(g2.groupName, ChannelType::FWD)] = std::vector({1});

        ff_dSenderMaster* sendMaster = new ff_dSenderMaster({{{g1.groupName, g2.groupName}, new ff_dCommTCPS({{ChannelType::FWD, g1},{ChannelType::FWD, g2}}, "G0")}}, &rt);

        gFarm.add_collector(new ff_dAsender(sendMaster));
        gFarm.add_workers({new WrapperOUT(new RealSource(ntask), 0, 1, 0, true)});

        gFarm.run_and_wait_end();
        return 0;
    } else if (atoi(argv[1]) == 1){
        rt[std::make_pair(g2.groupName, ChannelType::INT)] = std::vector({1});
        rt[std::make_pair(g3.groupName, ChannelType::FWD)] = std::vector({0});

        ff_dReceiverMaster *recMaster = new ff_dReceiverMaster({new ff_dCommTCP(g1, 1),new ff_dCommTCP(g1_2, 1)}, {{0, 0}});
        ff_dSenderMaster* sendMaster = new ff_dSenderMaster({{{g3.groupName}, new ff_dCommTCPS({{ChannelType::FWD, g3}}, "G1")}, {{g2_2.groupName}, new ff_dCommTCPS({{ChannelType::INT, g2_2}}, "G1")}}, &rt);

        // ff_dReceiverMaster *recMaster = new ff_dReceiverMaster({{false, new ff_dCommTCP(g1, 2, {{0, 0}})}}, {{0, 0}});
        // ff_dSenderMaster* sendMaster = new ff_dSenderMaster({{{g2.groupName, g3.groupName}, new ff_dCommTCPS({{ChannelType::INT, g2},{ChannelType::FWD, g3}}, &rt, "G1")}}, &rt);

        gFarm.add_emitter(new ff_dAreceiverH(recMaster, 2));
        gFarm.add_collector(new ff_dAsenderH(sendMaster));
        gFarm.cleanup_emitter();
		gFarm.cleanup_collector();
		
        auto s = new Source(2,0);
        auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF, s->alloctaskF)), new EmitterAdapter(s, 2, 0, {{0,0}}, true), true, true);

        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({std::make_pair(0,0)})});
        auto sink = new Sink(0);
        a2a.add_secondset<ff_node>({new ff_comb(new CollectorAdapter(sink, {0}, true), new WrapperOUT(new ForwarderNode(sink->serializeF, sink->freetaskF), 0, 1, 0, true)), new SquareBoxRight});

    } else if (atoi(argv[1]) == 2) {
        rt[std::make_pair(g1.groupName, ChannelType::INT)] = std::vector({0});
        rt[std::make_pair(g3.groupName, ChannelType::FWD)] = std::vector({0});

        ff_dReceiverMaster *recMaster = new ff_dReceiverMaster({new ff_dCommTCP(g2, 1),new ff_dCommTCP(g2_2, 1)}, {{1, 0}});
        ff_dSenderMaster* sendMaster = new ff_dSenderMaster({{{g3.groupName}, new ff_dCommTCPS({{ChannelType::FWD, g3}}, "G2")}, {{g1.groupName}, new ff_dCommTCPS({{ChannelType::INT, g1_2}}, "G2")}}, &rt);



        // ff_dReceiverMaster *recMaster = new ff_dReceiverMaster({{false, new ff_dCommTCP(g2, 2, {{1, 0}})}}, {{1, 0}});
        // ff_dSenderMaster* sendMaster = new ff_dSenderMaster({{{g1.groupName, g3.groupName}, new ff_dCommTCPS({{ChannelType::INT, g1},{ChannelType::FWD, g3}}, &rt, "G2")}}, &rt);

        gFarm.add_emitter(new ff_dAreceiverH(recMaster, 2));
        gFarm.add_collector(new ff_dAsenderH(sendMaster));
		gFarm.cleanup_emitter();
		gFarm.cleanup_collector();

		auto s = new Source(2,1);
		auto ea = new ff_comb(new WrapperIN(new ForwarderNode(s->deserializeF, s->alloctaskF)), new EmitterAdapter(s, 2, 1, {{1,0}}, true), true, true);

        a2a.add_firstset<ff_node>({ea, new SquareBoxLeft({std::make_pair(1,0)})}, 0, true);

        auto sink = new Sink(1);
		a2a.add_secondset<ff_node>({
									new ff_comb(new CollectorAdapter(sink, {1}, true),
												new WrapperOUT(new ForwarderNode(sink->serializeF, sink->freetaskF), 1, 1, 0, true), true, true),
									new SquareBoxRight
			                        }, true);

		
        
    } else {
        ff_dReceiverMaster *recMaster = new ff_dReceiverMaster({new ff_dCommTCP(g3, 2)});
        gFarm.add_emitter(new ff_dAreceiver(recMaster, 2));
        gFarm.add_workers({new WrapperIN(new StringPrinter(), 1, true)});

        gFarm.run_and_wait_end();
        return 0;
    }
    gFarm.add_workers({&a2a});
    gFarm.run_and_wait_end();
}
