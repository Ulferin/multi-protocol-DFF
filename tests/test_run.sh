type=${1:-tcp}
ntasks=${2:-10000}
protocol=${3:-""}
port=${4:-""}

ssh -tt federico@ffremote "./${type}_cl_test.out 3 $ntasks $protocol $port 2>&1 > ./log.3.${type}.txt" &
ssh -tt federico@ffremote "./${type}_cl_test.out 2 $ntasks $protocol $port 2>&1 > ./log.2.${type}.txt" &
ssh -tt federico@ffremote "./${type}_cl_test.out 1 $ntasks $protocol $port 2>&1 > ./log.1.${type}.txt" &
ssh -tt federico@ffremote "./${type}_cl_test.out 0 $ntasks $protocol $port 2>&1 > ./log.0.${type}.txt" &
