/*
 * (C) 2015 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef __MY_RPC
#define __MY_RPC

#include <margo.h>

/* visible API for example RPC operation */

typedef struct {
    float*   task;
} ff_rpc_in_t;

void ff_rpc(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc);

hg_return_t hg_proc_ff_rpc_in_t(hg_proc_t proc, void* data) {
    hg_return_t ret = HG_SUCCESS;
    ff_rpc_in_t* struct_data = (ff_rpc_in_t*) data;

    if(hg_proc_get_op(proc) == HG_DECODE)
        struct_data->task = new float();

    ret = hg_proc_raw(proc, struct_data->task, sizeof(float));
    if(ret != HG_SUCCESS) {
        printf("Serialization error.\n");
        return ret;
    }

    return ret;
}

void get_self_addr(margo_instance_id mid, char* addr_str) {
    hg_size_t addr_self_string_sz = 128;
    hg_addr_t addr_self;
    /* figure out first listening addr */
    hg_return_t hret = margo_addr_self(mid, &addr_self);
    if (hret != HG_SUCCESS) {
        fprintf(stderr, "Error: margo_addr_self()\n");
        margo_finalize(mid);
    }
    hret = margo_addr_to_string(mid, addr_str, &addr_self_string_sz,
                                addr_self);
    if (hret != HG_SUCCESS) {
        fprintf(stderr, "Error: margo_addr_to_string()\n");
        margo_addr_free(mid, addr_self);
        margo_finalize(mid);
    }
    margo_addr_free(mid, addr_self);
}

static void wait_fin(void* arg) {
    margo_instance_id* mid = (margo_instance_id*)arg;
    margo_wait_for_finalize(*mid);
}

static void finalize_xstream_cb(void* data) {
    ABT_xstream xstream = (ABT_xstream)data;
    printf("Joining...\n");
    ABT_xstream_join(xstream);
    printf("Freeing...\n");
    ABT_xstream_free(&xstream);
}

#endif /* __MY_RPC */
