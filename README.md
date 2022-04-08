# MIT6.824
 Lab for MIT6.824

## Lab 1: MapReduce
测试结果如下：
```bash
$ bash test-mr.sh       
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```


&emsp;&emsp;MapReduce 论文中的系统架构如下：
![avatar](./doc/mapreduce.jpg)


&emsp;&emsp;整个系统中 `worker` 和 `coordinator` 的交互状态包含下图所示的一系列情况：
![avatar](./doc/keyStatus.png)

&emsp;&emsp;本项目实际实现的代码在[src/mr](https://github.com/JiahaoHong1997/MIT6.824/tree/main/src/mr)目录下，分别对应
`coordinator`、`worker` 和 `RPC` 通信过程。详细的实现过程参见[MIT6.824 Lab1](https://jiahaohong1997.github.io/2022/04/06/MIT6.824%20Lab1/)