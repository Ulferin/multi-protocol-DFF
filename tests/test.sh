#!/bin/bash

# ----- test.sh -----
#
#
# ----- test.sh -----

SERVER_ADDR=$(grep ffremote /etc/hosts | awk '{print $1}')
LOCAL_ADDR=$(hostname -I | awk '{gsub(/ /,""); print}')

group=$1
node=${2:-local}
prot=${3:-tcp}
next_prot=${4:-tcp}

CONTACT_ADDR=""
[[ $node = "local" ]] && CONTACT_ADDR=$LOCAL_ADDR || CONTACT_ADDR=$SERVER_ADDR

case ${group} in
    "g3") ./ff_g3.out "${prot}://${LOCAL_ADDR}:36000";;
    "g2") echo "connecting this group to node: $CONTACT_ADDR"; ./ff_g2.out "${prot}://${CONTACT_ADDR}:36000" "${prot}://${LOCAL_ADDR}:35000";;
    "g1") echo "connecting this group to node: $CONTACT_ADDR"; ./ff_g1.out 10 "${prot}://${CONTACT_ADDR}:35000";;
    *) echo "usage: $0 <group> <local/remote> <protocol> [next_group_prot]"
esac
