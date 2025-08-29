package common

const (
	// Base10Mega For networking throughput in Mbps, (and only for networking), we divide by 1000*1000 (not 1024 * 1024) because
	// networking is traditionally done in base 10 units (not base 2).
	// E.g. "gigabit ethernet" means 10^9 bits/sec, not 2^30. So by using base 10 units
	// we give the best correspondence to the sizing of the user's network pipes.
	// See https://networkengineering.stackexchange.com/questions/3628/iec-or-si-units-binary-prefixes-used-for-network-measurement
	// NOTE that for everything else in the app (e.g. sizes of files) we use the base 2 units (i.e. 1024 * 1024) because
	// for RAM and disk file sizes, it is conventional to use the power-of-two-based units.
	Base10Mega = 1000 * 1000
)
