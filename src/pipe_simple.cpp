#include <iostream>
#include <string>
#include <abt.h>
#include <margo.h>
#include <assert.h>
#include <thread>
#include <chrono>

// RPC function used to send stream elements between external groups
void ff_rpc_comm(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc_comm);

// RPC function used to signal EOS to remote groups
void ff_rpc_shutdown_comm(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc_shutdown_comm);

typedef struct {
    hg_int32_t task;
} ff_rpc_in_t;


/**
 * @brief Packing/Unpacking routine needed to read/write data from/to Margo
 * buffer. It will be internally called by Margo whenever get_input/get_output
 * are called.
 */
hg_return_t hg_proc_ff_rpc_in_t(hg_proc_t proc, void* data) {
    hg_return_t ret = HG_SUCCESS;
    ff_rpc_in_t* struct_data = (ff_rpc_in_t*) data;

    hg_proc_hg_int32_t(proc, &struct_data->task);

    return ret;
}

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

    int* received =
        (int*)margo_registered_data(mid, info->id);

    (*received)++;
    printf("\rReceived: %d", *received);

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_comm)

void ff_rpc_shutdown_comm(hg_handle_t handle) {
    const struct hg_info*   info;
    margo_instance_id       mid;

    info = margo_get_info(handle);
    assert(info);
    mid = margo_hg_info_get_instance(info);
    assert(mid != MARGO_INSTANCE_NULL);

    margo_finalize(mid);
    
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ff_rpc_shutdown_comm);

int main(int argc, char** argv) {
    if(argc != 5) {
        printf("Please, provide ntask and port parameters.\n");
    } 

    // margo_set_environment(NULL);
    // ABT_init(0, NULL);
    

    int ntask{atoi(argv[1])};
    int rank{atoi(argv[2])};
    int mswait{atoi(argv[3])};

    const char* proto = argv[4];
    const char* addr = argv[5];
    std::cout << "Running rank " << rank << " with ntask " << ntask << " and address " << addr << "\n";

    if(rank == 0) {
        margo_instance_id mid = margo_init(proto, MARGO_SERVER_MODE, 1, -1);
        hg_id_t id = MARGO_REGISTER(mid, "ff_rpc", ff_rpc_in_t, void, NULL);
        margo_registered_disable_response(mid, id, HG_TRUE);
        hg_id_t shut_id = MARGO_REGISTER(mid, "ff_rpc_shut", void, void, NULL);
        margo_registered_disable_response(mid, shut_id, HG_TRUE);

        for (int i = 0; i < ntask; i++)
        {
            ff_rpc_in_t in;
            in.task = i;

            hg_handle_t h;
            hg_addr_t svr_addr;

            margo_addr_lookup(mid, addr, &svr_addr);
            assert(svr_addr);

            margo_create(mid, svr_addr, id, &h);
            margo_forward(h, &in);
            margo_destroy(h);

            // margo_thread_sleep(mid, mswait);
            // std::this_thread::sleep_for(std::chrono::milliseconds(mswait));

        }

        hg_handle_t h;
        hg_addr_t svr_addr;

        margo_addr_lookup(mid, addr, &svr_addr);
        assert(svr_addr);

        margo_create(mid, svr_addr, shut_id, &h);
        margo_forward(h, NULL);
        margo_destroy(h);

        margo_thread_sleep(mid, 10000);
        margo_finalize(mid);        

    }
    else {
        int received = 0;
        margo_instance_id mid = margo_init(argv[5], MARGO_SERVER_MODE, 1, -1);
        hg_id_t id = MARGO_REGISTER(mid, "ff_rpc", ff_rpc_in_t, void, ff_rpc_comm);
        margo_registered_disable_response(mid, id, HG_TRUE);
        margo_register_data(mid, id, &received, NULL);
        id = MARGO_REGISTER(mid, "ff_rpc_shut", void, void, ff_rpc_shutdown_comm);
        margo_registered_disable_response(mid, id, HG_TRUE);

        margo_wait_for_finalize(mid);

        printf("\nReceived: %d\n", received);
    }


    // ABT_finalize();

    return 0;
}