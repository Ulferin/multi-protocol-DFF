type=${1:-tcp}
ntasks=${2:-10000}
protocol=${3:-""}
port=${4:-""}

ssh -tt compute4 "./${type}_test.out 3 $ntasks $protocol $port 2>&1 > ./log3.${type}.txt"
ssh -tt compute3 "./${type}_test.out 2 $ntasks $protocol $port > /dev/null"
ssh -tt compute2 "./${type}_test.out 1 $ntasks $protocol $port > /dev/null"
ssh -tt compute1 "./${type}_test.out 0 $ntasks $protocol $port > /dev/null"
