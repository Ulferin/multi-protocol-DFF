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

#endif /* __MY_RPC */
