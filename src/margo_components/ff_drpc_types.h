#ifndef FF_DRPC_TYPES
#define FF_DRPC_TYPES

#include <margo.h>

#include <ff/distributed/ff_network.hpp>

//TODO: ff_endpoint_rpc in current state in only able to deal with
//      plugin+protocol pairs that accept a "port" field. This is not the
//      case, for example, for na+sm, which has problems in managing the
//      "protocol" field.

// Endpoint extension for RPC compatibility. It acts like an endpoint for the
// distributed version and only adds the possibility to specify the plugin and
// protocol to be used 
struct ff_endpoint_rpc : public ff_endpoint {
    // The protocol argument must be defined as a Mercury plugin+protocol string
    ff_endpoint_rpc(std::string addr, int port=-1, std::string protocol="ofi+tcp"):
                ff_endpoint::ff_endpoint(addr, port),
                protocol(std::move(protocol)) {
                    
        std::stringstream sstm;
        if(this->port < 0)
            sstm << this->protocol << this->address;
        else
            sstm << this->protocol << "://" << this->address << ":" << (this->port);
        margo_addr = sstm.str();
    }

    std::string protocol;
    std::string margo_addr;
};


/* --- MARGO RPCs declaration */

// RPC function used to send stream elements between external groups
void ff_rpc_comm(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc_comm);

// RPC function used to send stream elements between internal groups
void ff_rpc_internal_comm(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc_internal_comm);

// RPC function used to signal EOS to remote groups
void ff_rpc_shutdown_comm(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc_shutdown_comm);

void ff_rpc_shutdown_internal_comm(hg_handle_t handle);
DECLARE_MARGO_RPC_HANDLER(ff_rpc_shutdown_internal_comm);

/* --- MARGO RPCs declaration --- */


/* --- RPC types and packing routines --- */

typedef struct {
    message_t*   task;
} ff_rpc_in_t;


/**
 * @brief Packing/Unpacking routine needed to read/write data from/to Margo
 * buffer. It will be internally called by Margo whenever get_input/get_output
 * are called.
 */
hg_return_t hg_proc_ff_rpc_in_t(hg_proc_t proc, void* data) {
    hg_return_t ret = HG_SUCCESS;
    ff_rpc_in_t* struct_data = (ff_rpc_in_t*) data;

    switch (hg_proc_get_op(proc)) {

        case HG_ENCODE:
        {
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
        }        
        case HG_DECODE:
        { 
            message_t* task = new message_t();
            
            ret = hg_proc_int32_t(proc, &task->chid);
            if(ret != HG_SUCCESS)
                break;

            ret = hg_proc_int32_t(proc, &task->sender);
            if(ret != HG_SUCCESS)
                break;

            size_t len = 0;
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
        }
        case HG_FREE:
        {
            // TODO: check how to free memory here
            // delete struct_data->task;
            break;
        }
    }

    return ret;
}

/* --- RPC types and packing routines --- */

#endif /* FF_DRPC_TYPES */
