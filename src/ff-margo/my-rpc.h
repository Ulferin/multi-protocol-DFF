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

#include <ff/distributed/ff_network.hpp>

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
    message_t*   task;
} ff_rpc_in_t;


/**
 * @brief Packing/Unpacking routine needed to read/write data from/to Margo send
 * buffer. It will be internally called by Margo whenever get_input/get_output
 * are called.
 */
hg_return_t hg_proc_ff_rpc_in_t(hg_proc_t proc, void* data) {
    hg_return_t ret = HG_SUCCESS;
    ff_rpc_in_t* struct_data = (ff_rpc_in_t*) data;

    switch (hg_proc_get_op(proc)) {

        case HG_ENCODE:
            ret = hg_proc_int32_t(proc, &struct_data->task->chid);
            if(ret != HG_SUCCESS)
                break;

            ret = hg_proc_int32_t(proc, &struct_data->task->sender);
            if(ret != HG_SUCCESS)
                break;

            size_t len = struct_data->task->data.getLen();
            ret = hg_proc_hg_size_t(proc, &len);
            if(ret != HG_SUCCESS)
                break;

            ret = hg_proc_raw(proc, struct_data->task->data.getPtr(),
                        struct_data->task->data.getLen());
            if(ret != HG_SUCCESS)
                break;

            break;
        
        case HG_DECODE:
            message_t* task = new message_t();
            
            ret = hg_proc_int32_t(proc, &task->chid);
            if(ret != HG_SUCCESS)
                break;

            ret = hg_proc_int32_t(proc, &struct_data->task->sender);
            if(ret != HG_SUCCESS)
                break;

            size_t len;
            ret = hg_proc_hg_size_t(proc, &len);
            if(ret != HG_SUCCESS)
                break;

            char* buf = new char[len];
            ret = hg_proc_raw(proc, buf, len);
            if(ret != HG_SUCCESS)
                break;

            task->data.setBuffer(buf, len, true);
            struct_data->task = task;

            break;

        case HG_FREE:
            delete struct_data->task;
            break;

    }

    return ret;
}

/* --- RPC types and packing routines --- */

#endif /* __MY_RPC */
