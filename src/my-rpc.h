/**
 * @file my-rpc.h
 * @author Federico Finocchio
 * @brief Header file containing RPC calls declaration and RPC types and packing
 * routine definition.
 * @version 0.1
 * @date 2022-03-22
 * 
 * @copyright Copyright (c) 2022
 * 
 */


#ifndef __MY_RPC
#define __MY_RPC

#include <margo.h>

/* --- MARGO RPCs declaration */

// RPC function used to send stream elements between different groups
void ff_rpc(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc);

// RPC function used to signal EOS to remote groups
void ff_rpc_shutdown(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc_shutdown);

/* --- MARGO RPCs declaration --- */


/* --- RPC types and packing routines --- */

typedef struct {
    float*   task;
} ff_rpc_in_t;

typedef struct {
    int*  resp;
} ff_rpc_out_t;


// Packing/Unpacking routine needed to read/write data from/to Margo send buffer.
hg_return_t hg_proc_ff_rpc_in_t(hg_proc_t proc, void* data) {
    hg_return_t ret = HG_SUCCESS;
    ff_rpc_in_t* struct_data = (ff_rpc_in_t*) data;

    if(hg_proc_get_op(proc) == HG_DECODE)
        struct_data->task = new float();

    switch (hg_proc_get_op(proc))
    {
    case HG_FREE:
        delete struct_data->task;
        break;
    
    default:
        ret = hg_proc_raw(proc, struct_data->task, sizeof(float));
        if(ret != HG_SUCCESS) {
            printf("Serialization error.\n");
            return ret;
        }
        break;
    }

    return ret;
}

// Packing/Unpacking routine needed to read/write data from/to Margo send buffer.
hg_return_t hg_proc_ff_rpc_out_t(hg_proc_t proc, void* data) {
    hg_return_t ret = HG_SUCCESS;
    ff_rpc_out_t* struct_data = (ff_rpc_out_t*) data;

    if(hg_proc_get_op(proc) == HG_DECODE)
        struct_data->resp = new int();

    switch (hg_proc_get_op(proc))
    {
    case HG_FREE:
        delete struct_data->resp;
        break;
    
    default:
        ret = hg_proc_raw(proc, struct_data->resp, sizeof(int));
        if(ret != HG_SUCCESS) {
            printf("Serialization error.\n");
            return ret;
        }
        break;
    }

    return ret;
}

/* --- RPC types and packing routines --- */

#endif /* __MY_RPC */
