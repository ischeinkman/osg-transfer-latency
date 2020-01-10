# Gets the list of interfaces
LOCALS=sudo ifconfig | \
    # Only the IP address lines 
    grep inet | \
    # Extract the IPv4 addresses from the lines
    sed "s/inet \([^ ]*\) .*/\1/" | \
    # And IPv6 
    sed "s/inet6 \([^ ]*\) .*/\1/" | \
    # And create a bash list from the text
    xargs echo

# Clean up old root
tc qdisc del dev ens785 root
# Sets up a "root" in the tc rule "tree" that by default has 3 branches
tc qdisc add dev ens785 root handle 1: prio

# Sets up all traffic to be directed to the node of index 2 
tc filter add dev ens785 protocol all parent 1: prio 2 u32 match ip dst 0.0.0.0/0 flowid 1:2
tc filter add dev ens785 protocol all parent 1: prio 2 u32 match ip protocol 1 0xff flowid 1:2

for lcla in $LOCALS
do 
    for lclb in $LOCALS 
    do 
        # Set traffic between 2 local IPs to be directed to flow 1
        tc filter add dev ens785 protocol ip parent 1: prio 1 u32 match ip dst $lcla ip src $lclb 0xffff flowid 1:1
    done
done
# Adds a delay to node 2
tc qdisc add dev ens785 parent 1:2 handle 20: netem delay $1
# Node 1 gets the default without latency 
tc qdisc add dev ens785 parent 1:1 handle 10: sfq

#Helpful reference: https://serverfault.com/questions/906458/network-shaping-using-tc-netem-doesnt-seem-to-work/906499#906499