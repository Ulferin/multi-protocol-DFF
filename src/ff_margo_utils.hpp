
#ifndef FF_MARGO_UTILS
#define FF_MARGO_UTILS

#include <margo.h>
#include <thread>


// Utily function to retrieve address of listening endpoint using Margo interface
void get_self_addr(margo_instance_id* mid, char* addr_str) {
    hg_size_t addr_self_string_sz = 128;
    hg_addr_t addr_self;
    

    hg_return_t hret = margo_addr_self(*mid, &addr_self);
    if (hret != HG_SUCCESS) {
        fprintf(stderr, "Error: margo_addr_self()\n");
        margo_finalize(*mid);
    }
    hret = margo_addr_to_string(*mid, addr_str, &addr_self_string_sz,
                                addr_self);
    if (hret != HG_SUCCESS) {
        fprintf(stderr, "Error: margo_addr_to_string()\n");
        margo_addr_free(*mid, addr_self);
        margo_finalize(*mid);
    }
    margo_addr_free(*mid, addr_self);
}

// Wrapper function to pass an ABT_thread to wait for Margo to be finalized
static void wait_fin(void* arg) {
    margo_instance_id* mid = (margo_instance_id*)arg;
    margo_trace(*mid, "Waiting to finalize...");
    margo_wait_for_finalize(*mid);
    margo_trace(*mid, "Finalization done.");
}

// Utility function to gracefully terminate xstream object
static void finalize_xstream_cb(void* data) {
    ABT_xstream xstream = (ABT_xstream)data;
    ABT_xstream_join(xstream);
    ABT_xstream_free(&xstream);
}

#endif