package helpers

import "runtime"

func GetAvailableMemory() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	// You can use memStats.Sys or another appropriate memory metric.
	// Consider leaving some memory unused for other processes.
	availableMemory := memStats.Sys - memStats.HeapInuse
	return availableMemory
}
