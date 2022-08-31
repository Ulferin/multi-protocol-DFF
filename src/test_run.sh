type=$1

ssh -tt federico@ffremote "/home/federico/programming/multi-protocol-DFF/src/cl_test.out 3 2>&1 > ~/programming/multi-protocol-DFF/src/g3_$type.txt" &
ssh -tt federico@ffremote "/home/federico/programming/multi-protocol-DFF/src/cl_test.out 2 2>&1 > ~/programming/multi-protocol-DFF/src/g2_$type.txt" &
ssh -tt federico@ffremote "/home/federico/programming/multi-protocol-DFF/src/cl_test.out 1 2>&1 > ~/programming/multi-protocol-DFF/src/g1_$type.txt" &
ssh -tt federico@ffremote "/home/federico/programming/multi-protocol-DFF/src/cl_test.out 0 2>&1 > ~/programming/multi-protocol-DFF/src/g0_$type.txt" &
