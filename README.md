# Enabling multi-protocol support in FastFlow distributed runtime
## Master's Degree in Computer Science
---

Federico Finocchio: f.finocchio@studenti.unipi.it
Academic Year: 2021/2022

**Abstract**  
A shift toward distributed computing is being recorded in order to address storage and computational needs of the new generation of data-intensive applications. This new programming environment requires new tools and frameworks, as well as new abstractions, to allow programmers to provide applications that deal with real-time decisions in an accurate and actionable way. Portability, performance, and programmability are provided to the application programmer at different levels of abstraction. However, handling the heterogeneity of a distributed environment remains challenging and requires special effort to correctly and efficiently exploit parallel and distributed resources. The thesis provides an in-depth study of the communication landscape in the distributed environment, proposing a multi-protocol extension to the existing distributed runtime of FastFlow, a C++ structured parallel and distributed programming framework. Our extension targets the runtime system of FastFlow, providing a flexible and extensible way of defining multi-protocol applications. In contrast to existing frameworks, we propose true multi-protocol support, which allows remote nodes to connect using multiple transport protocols (e.g., MPI, TCP, Margo), effectively enabling heterogeneity of compute nodes. A set of tests validate the implemented extension and evaluates its performance against the existing distributed runtime of FastFlow.

---

## Project Structure

```bash
├───README
├───src
│   ├───ff_dManager.hpp
│   ├───ff_dManagerI.hpp
│   ├───ff_dMPreceiver.hpp
│   ├───ff_dMPsender.hpp
│   ├───ff_dTransportType.hpp
│   ├───ff_dTransportTypeI.hpp
│   └───margo_components                
│       ├───ff_dCommunicator.hpp        #
│       ├───ff_drpc_types.h             # Plugin implementation for Margo transport
│       └───ff_margo_components.hpp     #
│
└───tests
    ├───test_mpi.sh         #
    ├───test_tcp.sh         # Testing scripts
    ├───test_run.sh         #
    ├───...
    ├───mpi_performance.cpp #
    ├───tcp_performance.cpp #
    ├───test_a2a_mpi.cpp    # Tests as specified in thesis report
    ├───test_a2a_tcp.cpp    #
    └───test_a2a_rpc.cpp    #
```

---

## Running the experiments

We provide bash scripts to test the implemented functionalities and get statistics on their execution. The implemented functionalities are an extension of the existing FastFlow distributed runtime classes. The tests can be compiled using the provided `makefile` in the `tests` folder. No additional requirements are needed more than the FastFlow library for both TCP and MPI tests. In order to test Margo-related components, the requirements are related to the dependencies as specified in [Margo Github repository](https://github.com/mochi-hpc/mochi-margo).

The sample application is relative to the FastFlow distributed concurrency graph as follows:
<img src="https://eu2.contabostorage.com/7b9b9863ab44439fbf94633415e63c03:university/tesi%2FCommunication%20-%20distmem.png" alt="distributed-memory-app" width="400"/>

### Performance Tests

The tests are related to the assessment of possible overheads introduced by the implemented extension. By using the `tests/test_tcp.sh` and `tests/test_mpi.sh` scripts, the user can run performance tests while using the TCP and MPI protocols for all the communications in the sample application. 

Please refer to the specific test files for detailed information about the parameters.

Various parameters are available for the two tests:

- test type: used to specify which of the implemented version to run. Like original TCP/MPI implementation or TCP/MPI extension
- task number: number of tasks generated by the Source node in the application, note that the LNode will generate 4 tasks per item received from the Source
- message size: size in bytes to use for each of the tasks generated by the LNodes
- ms wait: ms of wait time for the LNode and RNode

**Example usage**

```bash
$ cd tests
$ make tcp_performance.out
$ ./test_tcp.sh <type> <ntask> <msg_size> <lmswait> <rmswait>
```

The execution via the script file provided will produce a file which can be used to check performances and statistics. The produced file are relative to:

- execution statistics: this is a dump of the console outputs produced by the execution of the `{test_type}_performance.out` file. All the outputs are written in a `.txt` file saved in `tests/tcpres` (for TCP related tests) and `tests/mpires` (for MPI related tests).

**Results obtained with the implemeted functionalities**
In the following, the main results we obtained while testing the overhead introduced by the implemented functionalities when compared to the original implementation by FastFlow. We notice that no notable overheads are introduced for two kinds of tests:

- increasing message size, with 100k total messages
- increasing number of messages, with fixed 1Kb message size

<img src="https://eu2.contabostorage.com/7b9b9863ab44439fbf94633415e63c03:university/tesi%2FcomparisonMPI.png" alt="mpi-overhead" width="450"/>
<img src="https://eu2.contabostorage.com/7b9b9863ab44439fbf94633415e63c03:university/tesi%2FcomparisonTCP.png" alt="tcp-overhead" width="450"/>


### Protocol-specific tests

The tests for the protocol-specific functionalities are the tests used to check the correctness of the provided extension. They use a simpler concurrency graph with the same structure of the original application. The tests can be run by using the `tests/test_run.sh` script. Only TCP and RPC functionalities can be tested using this script file.

Parameters are:

- type: either tcp or rpc, it specifies which of the implemented transports to use
- number of tasks: number of tasks to be generated by the Source node. Note that each LNode will generate 2 tasks for each one received by the Source
- protocol: the protocol to use in case *rpc* is specified as test type
- port: the port to use for the specified protocol

**Example usage**
```bash
$ cd tests
$ make rpc_test.out
$ ./test_run rpc 10000 "ofi+sockets" 8000
```

The execution will produce a file which can be used to check the statistics of the current execution, only for the last stage in the sample application. All the outputs are written in a `.txt` file.

### Case-study test
This test refers to the analyzed case study application, connecting different computing environments such as a personal laptop, an HPC cluster and a publicly reachable virtual machine. The application structure is as follows:
<img src="https://eu2.contabostorage.com/7b9b9863ab44439fbf94633415e63c03:university/tesi%2FCommunication%20-%20case.png" alt="case-study-app" width="900"/>

The case-study application shows the effective use and functionality of the presented multi-protocol implementation. Two protocols are used by means of a single programming framework. The input/output connections toward/from the HPC cluster are performed via TCP due to internal limitations on the reachability of the cluter nodes.