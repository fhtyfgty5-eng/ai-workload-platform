package workflow

import "time"

// Clock 隔离当前时间和等待能力，使重试与超时测试不依赖真实时间流逝。
type Clock interface {
	// Now 返回当前时间。
	Now() time.Time
	// After 返回在指定时长后可接收值的通道。
	After(time.Duration) <-chan time.Time
}

// RealClock 使用操作系统的真实时间实现 Clock。
type RealClock struct{}

// Now 返回操作系统当前时间。
func (RealClock) Now() time.Time { return time.Now() }

// After 使用真实定时器等待指定时长。
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
