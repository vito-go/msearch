# msearch
以本地文件为基础的搜索技术。提供增、删、查API（简单的替代mysql。）
存储数据： key-values, 一个key对应多个values。

基于MMAP(Memory mapping)技术的，将硬盘映射到内存，轻松映射超过100G硬盘空间至内存（不影响实际内存占用），相当与无限内存。
查询可性能超越百万QPS。
用法及性能详见测试文件： msearch_test.go
