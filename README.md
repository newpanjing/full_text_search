# 基于Python的全文检索引擎

基于Lucene基本原理来实现

+ 分词
+ 字典
+ 文档
+ 索引

分词采用`jieba`

## 索引原理

`fts.index` 这个文件记录了 关键词 和关键词对应的文档索引位置

文档索引 是文档在 `fts.doc`中的区块索引，通过这个索引可以计算出数据块的准确位置

例如： 文档索引=1

取文件指针开始和结束：

```python
start = 1 - 1 * 4098

end = 1 * 4098
```

搜索原理：

1. 加载索引文件到内存 fts.index
2. 搜索的关键词通过jieba分词，然后得到的词在内存中查找 fts.index符合的关键词
3. 查找到的关键词取到`pos`，这个`pos`就是 `fts.doc`的索引位置
4. 通过索引位置取到文档

通过上面这个步骤就可以完美的实现全文检索

例如索引的词是：

```
雷军向金山所有员工赠予每人600股的股票
```

分词的关键字有： 雷军、金山....

然后我们搜索词是：

```
雷军是金山的的董事长吗？
```

这时候分词 有3个关键字

雷军、金山、董事长

通过3个任意关键词就可以找到 索引中的关键词，然后还可以根据击中次数得到分数score

最后根据score 排序

## 目前实现的功能

只有最基本的内存全文检索，没有排序、没有分页

后期可以优化成索引用b+tree这一类的做，然后减少文件io次数，等等