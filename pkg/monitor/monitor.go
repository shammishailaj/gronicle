package monitor

import (
	"github.com/shirou/gopsutil/v3/process"
	"log"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

// TaskMetrics holds resource metrics for a task execution.
type TaskMetrics struct {
	CPUUsage    float64 `json:"cpu_usage"`
	RAMUsage    float64 `json:"ram_usage"`
	DiskUsage   float64 `json:"disk_usage"`
	LoadAverage float64 `json:"load_average"`
	GPUUsage    float64 `json:"gpu_usage"` // Placeholder for future GPU monitoring
	RecordedAt  time.Time
}

// CollectMetrics gathers system metrics during task execution.
func CollectMetrics() TaskMetrics {
	cpuUsage, _ := cpu.Percent(0, false)
	memStats, _ := mem.VirtualMemory()
	diskStats, _ := disk.Usage("/")
	loadAvg, _ := load.Avg()

	return TaskMetrics{
		CPUUsage:    cpuUsage[0],
		RAMUsage:    memStats.UsedPercent,
		DiskUsage:   diskStats.UsedPercent,
		LoadAverage: loadAvg.Load1,
		GPUUsage:    0.0, // Placeholder for future GPU usage
		RecordedAt:  time.Now(),
	}
}

type ProcessMetrics struct {
	CPUUsage   float64 `json:"cpu_usage"`
	RAMUsage   float64 `json:"ram_usage"`
	DiskUsage  float64 `json:"disk_usage"`
	RecordedAt time.Time
}

// CollectProcessMetrics gathers metrics for the specific task's process (by PID).
func CollectProcessMetrics(pid int32) ProcessMetrics {
	proc, err := process.NewProcess(pid)
	if err != nil {
		log.Printf("Failed to monitor process (PID: %d): %v", pid, err)
		return ProcessMetrics{RecordedAt: time.Now()}
	}

	// Collect CPU, memory, and disk usage (disk stats simulated)
	cpuPercent, _ := proc.CPUPercent()
	memPercent, _ := proc.MemoryPercent()
	diskUsage := 0.0 // Placeholder - per-process disk usage is typically not directly available

	return ProcessMetrics{
		CPUUsage:   cpuPercent,
		RAMUsage:   float64(memPercent),
		DiskUsage:  diskUsage,
		RecordedAt: time.Now(),
	}
}
