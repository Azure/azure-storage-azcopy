package hostinfo

func PhysicalMemoryGB(bytes uint64) int {
	const gib uint64 = 1 << 30
	gigabytes := bytes / gib
	if bytes%gib >= gib/2 {
		gigabytes++
	}
	if gigabytes > uint64(^uint(0)>>1) {
		return -1
	}
	return int(gigabytes)
}
