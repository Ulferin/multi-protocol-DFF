type=${1:-tcp}
ntasks=${2:-10000}
lmswait=${3:-0}
rmswait=${4:-0}
protocol=${5:-"ofi+sockets"}

ssh -tt federico@ffremote "/home/federico/programming/multi-protocol-DFF/src/${type}_cl_test.out 3 $ntasks $lmswait $rmswait $protocol 2>&1 > ~/programming/multi-protocol-DFF/src/g3_$type.txt" &
ssh -tt federico@ffremote "/home/federico/programming/multi-protocol-DFF/src/${type}_cl_test.out 2 $ntasks $lmswait $rmswait $protocol 2>&1 > ~/programming/multi-protocol-DFF/src/g2_$type.txt" &
ssh -tt federico@ffremote "/home/federico/programming/multi-protocol-DFF/src/${type}_cl_test.out 1 $ntasks $lmswait $rmswait $protocol 2>&1 > ~/programming/multi-protocol-DFF/src/g1_$type.txt" &
ssh -tt federico@ffremote "/home/federico/programming/multi-protocol-DFF/src/${type}_cl_test.out 0 $ntasks $lmswait $rmswait $protocol 2>&1 > ~/programming/multi-protocol-DFF/src/g0_$type.txt" &
