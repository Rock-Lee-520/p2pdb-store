中文 | [English](./README-EN.md)
# p2pdb-store

这是一个p2pdb-server 的存储实现,包含memory 内存存储驱动及 sqlite 文件存储驱动，你也可以根据该规范自行对接任一一种数据库,如postgresql、clickhouse、TDengine等开源数据库。

## Go 文档

* [p2pdb-store godoc](https://godoc.org/github.com/kkguan/p2pdb-store)

## 致谢

**p2pdb-store** 内存存储的代码来源于go-mysql-server，其组织为dolthub组织,这里感谢dolthub组织的付出,sqlite的驱动因未寻找到合适实现,由p2pdb维护团队自行开发。

## 部分代码参考来源
[go-mysql-server](http://github.com/kkguan/p2pdb-store):遵守Apache License 2.0协议

## License

Apache License 2.0, see [LICENSE](/LICENSE)
