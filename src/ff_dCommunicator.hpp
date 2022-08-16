#ifndef FFDCOMM_I
#define FFDCOMM_I

#include "ff_dAreceiver.hpp"
#include <ff/ff.hpp>
#include <ff/distributed/ff_network.hpp>

class ff_dCommRPC: public ff_dCommunicator {

protected:
    void init_ABT() {
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_SPSC,
            ABT_FALSE, &pool_e1);
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &pool_e1,
            ABT_SCHED_CONFIG_NULL, &xstream_e1);
    }


    void init_mid(const char* address, margo_instance_id* mid) {
        na_init_info na_info = NA_INIT_INFO_INITIALIZER;
        na_info.progress_mode = busy ? NA_NO_BLOCK : 0;

        hg_init_info info = {
            .na_init_info = na_info
        };

        margo_init_info args = {
            .json_config   = NULL,      /* const char*          */
            .progress_pool = pool_e1,   /* ABT_pool             */
            .rpc_pool      = pool_e1,   /* ABT_pool             */
            .hg_class      = NULL,      /* hg_class_t*          */
            .hg_context    = NULL,      /* hg_context_t*        */
            .hg_init_info  = &info       /* struct hg_init_info* */
        };

        //DESIGN: this should be changed in case we want a "listening" communicator
        *mid = margo_init_ext(address, MARGO_SERVER_MODE, &args);
        assert(*mid != MARGO_INSTANCE_NULL);
        // margo_set_log_level(*mid, MARGO_LOG_TRACE);

        // Check if the listening address is the requested one
        char addr_self_string[128];
        get_self_addr(mid, addr_self_string);
        fprintf(stderr, "# accepting RPCs on address \"%s\"\n",
            addr_self_string);
    }


    void register_rpcs(margo_instance_id* mid, void* data) {
        hg_id_t id = MARGO_REGISTER(*mid, "ff_rpc", ff_rpc_in_t, void, ff_rpc_comm);
        // NOTE: we actually want a response in the non-blocking version
        //DESIGN: this should change when supporting also sender functionalities
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, data, NULL);

        id = MARGO_REGISTER(*mid, "ff_rpc_shutdown",
                void, void, ff_rpc_shutdown_comm);
        margo_registered_disable_response(*mid, id, HG_TRUE);
        margo_register_data(*mid, id, data, NULL);

        if (internal) {
            hg_id_t id = MARGO_REGISTER(*mid, "ff_rpc_internal",
                    ff_rpc_in_t, void, ff_rpc_internal_comm);
            margo_registered_disable_response(*mid, id, HG_TRUE);
            margo_register_data(*mid, id, data, NULL);

            id = MARGO_REGISTER(*mid, "ff_rpc_shutdown_internal",
                    void, void, ff_rpc_shutdown_internal_comm);
            margo_registered_disable_response(*mid, id, HG_TRUE);
            margo_register_data(*mid, id, data, NULL);
        }
    }


public:
    ff_dCommRPC(bool internal, bool busy, std::vector<ff_endpoint_rpc*> endRPC):
        internal(internal), busy(busy), endRPC(std::move(endRPC)) {}

    //NOTE: probably init function can be inserted into the constructor call
    //      we don't need to defer the initialization in the svc_init of the
    //      FastFlow node
    virtual void init(ff_monode_t<message_t>* data) {
        init_ABT();
        for (auto &&addr: this->endRPC)
        {
            margo_instance_id* mid = new margo_instance_id(); // FIXME: is it really necessary to create a new oject?
            init_mid(addr->margo_addr.c_str(), mid);
            register_rpcs(mid, data);
            mids.push_back(mid);
        }
    }

    virtual void listen() {
        std::vector<ABT_thread*> threads;

        for (auto &&mid : mids)
        {
            ABT_thread* aux = new ABT_thread();
            ABT_thread_create(pool_e1, wait_fin, mid, NULL, aux);
            threads.push_back(aux);
        }

        finalize_xstream_cb(xstream_e1);
        ABT_pool_free(&pool_e1);
    }

    virtual void send() {
        return;
    }

    virtual void finalize() {
        for (auto &&mid : mids)
            {
                margo_finalize(*mid);
            }
    }


protected:
    bool                            internal;
    int                             busy;
    std::vector<ff_endpoint_rpc*>   endRPC;
    std::vector<margo_instance_id*> mids;
    ABT_pool                        pool_e1;
    ABT_xstream                     xstream_e1;

};


void ff_rpc_comm(hg_handle_t handle) {
    hg_return_t             hret;
    ff_rpc_in_t             in;
    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);
    
    //NOTE: this may cause problem while using ucx
    // hg_size_t size = 128;
    // char addr_string[128];
    // margo_addr_to_string(mid, addr_string, &size, info->addr);
    // printf("Received from: %s\n", addr_string);

    ff_dAreceiver* receiver =
        (ff_dAreceiver*)margo_registered_data(mid, info->id);

    receiver->forward(in.task, false);

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_comm)


void ff_rpc_internal_comm(hg_handle_t handle) {
    hg_return_t             hret;
    ff_rpc_in_t             in;
    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    ff_dAreceiver* receiver =
        (ff_dAreceiver*)margo_registered_data(mid, info->id);

    receiver->forward(in.task, true);

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_internal_comm)


void ff_rpc_shutdown_comm(hg_handle_t handle) {
    std::cout << "Received an external shutdown!\n";
    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    ff_dAreceiver* receiver =
        (ff_dAreceiver*)margo_registered_data(mid, info->id);
    
    receiver->registerEOS(false);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown_comm);


void ff_rpc_shutdown_internal_comm(hg_handle_t handle) {
    std::cout << "Received an internal shutdown!\n";

    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    ff_dAreceiver* receiver =
        (ff_dAreceiver*)margo_registered_data(mid, info->id);
    
    receiver->registerEOS(true);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown_internal_comm);


#endif //FFDCOMM