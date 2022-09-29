ttype=${1:-MPISP}
ntasks=${2:-1000}
msgsize=${3:-1024}
lmswait=${4:-0}
rmswait=${5:-0}

mpirun -n 4 --hostfile myhosts --map-by node sh -c "./${ttype}_perf.out 0 $ntasks $msgsize $lmswait $rmswait >> /home/f.finocchio/multi-protocol-DFF/tests/mpires/${ttype}.\$OMPI_COMM_WORLD_RANK.txt"
# mpirun --hostfile myhostsINOUT -n 4 --map-by node sh -c './caseMPIINOUT.out 3 25000 1024 0 0'