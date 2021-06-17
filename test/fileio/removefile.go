package main

import (
	"fmt"
	"os"
)

func main() {
	// 创建文件
	fp, err := os.Create("./demo.txt") // 如果文件已存在，会将文件清空。
	fmt.Println(fp, err)               // &{0xc000076780} <nil>
	fmt.Printf("%T\n", fp)               // *os.File 文件指针类型

	if err != nil {
		fmt.Println("文件创建失败。")
		//创建文件失败的原因有：
		//1、路径不存在  2、权限不足  3、打开文件数量超过上限  4、磁盘空间不足等
		return
	}

	// defer延迟调用
	defer fp.Close() //关闭文件，释放资源。
}