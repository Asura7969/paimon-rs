# TODO LIST

## 1、优化`limit`实现

**问题**：当前`limit`实现需要读取当前`snapshot`中所有数据，`merge`后再`limit`，导致limit性能较差

**解决方案**：先读取`base`数据, `merge`后如果满足`limit`个数，直接返回, 不满足则读取`delta`数据，`merge`后，再判断是否满足`limit`条数，减少文件读取量和`merge`数据量