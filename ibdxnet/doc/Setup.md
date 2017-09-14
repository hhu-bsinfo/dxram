All notes here are based on using Mellanox hardware.

# Install
* Get drivers and stack, ensure the distribution and version is _EXACTLY_ 
matching what you have installed: 
[Mellanox Homepage](http://www.mellanox.com/page/software_overview_ib)
* Run installer
* Run self test: hca_self_test.ofed

This should all kernel modules and tools necessary to use the included 
ibverbs library.

Make sure to run at least one subnet manager (if you don't have a managed
switch already running one).

# Setup IP over IB
Not really necessary for this project but might be useful for testing/evalution:
Add the following snippet to `/etc/network/interfaces` (here, the adapter gets 
the address 10.0.1.51 assigned):
```
auto ib0
iface ib0 inet static
	address 10.0.1.51
	netmask 255.255.255.0
```
Restart the driver (or machine) after this.

# Status/Monitor Commands

* ibstat: list available infiniband devices and state
* ibstatus: show ib device status
* ibnodes: show available ib nodes in network
* iblinkinfo: show switch layout with connected nodes, identifiers and total uplink

# Perf
Before running any commands, align CPU freq to performance mode on all machines:
```
for CPUFREQ in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do [ -f $CPUFREQ ] || continue; echo -n performance > $CPUFREQ; done
```