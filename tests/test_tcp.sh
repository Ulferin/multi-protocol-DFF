type=${1:-TCPSP}
ntasks=${2:-1000}
msgsize=${3:-500}
lmswait=${4:-0}
rmswait=${5:-0}

ssh -tt compute4 "./${type}_performance.out 3 $ntasks $msgsize $lmswait $rmswait >> ./tcpres/${type}.txt" &
ssh -tt compute3 "./${type}_performance.out 2 $ntasks $msgsize $lmswait $rmswait > /dev/null" &
ssh -tt compute2 "./${type}_performance.out 1 $ntasks $msgsize $lmswait $rmswait > /dev/null" &
ssh -tt compute1 "./${type}_performance.out 0 $ntasks $msgsize $lmswait $rmswait > /dev/null" &