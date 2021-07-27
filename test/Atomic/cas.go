package main

import (
	"fmt"
	"sync/atomic"
)

func TestMyAtomic()  {
	// 写入
	var a int64
	atomic.StoreInt64(&a,10)
	fmt.Println("写入a：",a)
	// 修改
	//atomic.AddInt64(&a,1)
	// 读取
	b := atomic.LoadInt64(&a)
	fmt.Println("读取a：",b)
	// 交换，非cas，直接赋予新值
	old := atomic.SwapInt64(&a,11)
	fmt.Println("交换后旧值：",old)
	fmt.Println("新值：",atomic.LoadInt64(&a))
	// 比较并交换，只有跟期望值相等才会交换
	flag := atomic.CompareAndSwapInt64(&a,10,10)
	fmt.Println("比较并交换：",flag)
}
func main()  {
	TestMyAtomic()
}
