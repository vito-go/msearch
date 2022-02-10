# msearch

- 基于本地文件(非内存)，实现海量数据的存储、搜索功能 ，查询性能近百万QPS。 存储数据类型: key-values, 一个key对应多个value。

- 接口方法 支持操作：Add, Del, Get等满足基本需求。删除value后，有新的value插入自动寻找空缺位，避免空间浪费。

  > 例如一个一条数据key1包含3个value： aaa bbb ccc， 删除bbb后变为,aaa _ ccc, 此时新插入一个value ddd，则变为 aaa ddd ccc. 如果插入长度超过空缺位长度，自动向后寻找合适的位置. 插入dddd 变为aaa _ ccc dddd，此时再插入fff，变为aaa fff ccc ddddd

- 可以借助kafka等实现分布式。

- 使用场景: 例如, 做根据昵称实时搜索好友、粉丝，粉丝用户可能数量100-100万粉丝不等。 输入一个用户的粉丝的昵称。然后进行模糊匹配搜索。

基于MMAP(Memory mapping)技术，将硬盘映射到内存，轻松映射超过100G硬盘空间至内存（不影响实际内存占用），相当于无限内存。

## Support
- 支持linux，mac os， 不支持windows（TODO）

## Usage
```go
package main

import (    
	"fmt"
	"github.com/vito-go/msearch"
)

func main() {
	fileName := "test.msearch" // 底层文件名
	ms, err := msearch.NewMsearch(fileName, 0)
	if err != nil {
		panic(err)
	}
	user := "example@example.com"
	userLi := "userLi@example.com"
	values := []string{"abc429298@example.com", "abc429179@example.com", "abc429178@example.com", "abc429177@example.com", "abc429176@example.com", "kadhx11@example.com", "kadhx1@example.com", "1101010022@example.com"}
	err = ms.Add(user, values...) // 添加一组values
	if err != nil {
		panic(err)
	}
	fmt.Println(ms.Get(user))               // 获取一个key对应的一组values   
	// 添加一个value
	if err=ms.Add(userLi, "aaa@163.com|nickname1");err!=nil{
		panic(err)
	} 
	ms.Del(user, values[:len(values)-2]...) // 删除多个value
	fmt.Println(ms.Get(user))               // 获取一组values
	fmt.Println(ms.Get(`userNil`))          // 获取一组values,如果没有values返回空数组。 
}

```