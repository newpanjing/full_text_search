import os
import time
import uuid

import jieba
import struct
import json
from io import BytesIO
import pickle
from numba import jit
from numba.roc.decorators import autojit


class LowSearch(object):
    """
    为啥叫LowSearch？

    因为性能、效率太差，只是对Lucene的大概原理实现
    """

    # 一条数据在文档的大小，4 是头文件，代表当前的这个数据包的大小，最大是不超过4*1024
    DOC_HEADER_LEN = 2
    DOC_CHUNK = 4 * 1024 + DOC_HEADER_LEN

    # 高亮开始标签
    PRE_TAG = "<em>"
    # 高亮结束标签
    POST_TAG = "</em>"

    indexs = {

    }

    def __init__(self, index_dir, index_fields):
        if not os.path.exists(index_dir):
            print("索引目录不存在，创建目录")
            os.mkdir(index_dir)

        self.index_dir = index_dir
        self.index_fields = index_fields

        # 初始化索引
        self._init_index()

        if os.path.exists(self.index_file):
            with open(self.index_file, 'rb') as f:
                self.indexs = pickle.load(f)

    def _init_index(self):
        """
        初始化索引文件
        :return:
        """

        # 文件有字典文件、文档文件，指针文件(可以做到没一块都不浪费，支持删除和修改)

        # 初步计划一条记录最大为4kb，理论上一个G可以容纳索引记录数为：262144

        # 记录的是字典，比如：中国的高铁速度为什么能这么快？，根据分词：中国、高铁、速度，把这些存入字典文件，key=词、value=doc中的索引位置
        index_file = os.path.join(self.index_dir, "fts.index")
        self.index_file = index_file

        # 文档文件，记录的是文档的数据，json格式
        doc_file = os.path.join(self.index_dir, "fts.doc")
        self.doc_file = doc_file

        # 记录下个坐标，和空闲坐标，默认用空闲坐标
        pos_file = os.path.join(self.index_dir, "fts.pos")
        self.pos_file = pos_file

    def _index_doc(self, keywords, doc):
        """
        写入索引
        :param keywords:
        :param doc:
        :return:
        """

        _pk = None
        if '_pk' in doc:
            _pk = doc.get('_pk')
        else:
            _pk = uuid.uuid4().hex
            doc['_pk'] = _pk

        # 写入文档，返回指针位置
        index = self._write_doc(doc)
        # 写索引
        self._write_index(keywords, index)
        return _pk

    def _write_index(self, keywords, index):
        """
        写入关键字在文档中对应的块索引
        :param keywords:
        :param index:
        :return:
        """

        # 正确的做法是 用二进制写入关键字对应的索引列表，这里是偷懒的做法，直接序列化对象

        for key in keywords:

            if key in self.indexs:
                val = self.indexs[key]
                val.append(index)
            else:
                self.indexs[key] = [index]

        # TODO 可以加入延时写入，减少文件io次数
        with open(self.index_file, 'wb') as f:
            pickle.dump(self.indexs, f)

        # pickle.dump()

    def _write_doc(self, doc):
        """
        写入文档
        :param doc:
        :return:
        """
        # 一个数据块的大小为4kb
        chunk = self.DOC_CHUNK

        with open(self.doc_file, 'ab') as file:

            buffer = BytesIO()
            bs = bytes(json.dumps(doc), 'utf8')
            data_len = len(bs)
            if data_len > chunk - self.DOC_HEADER_LEN:
                raise ValueError(f"写入的块不能大于{chunk}字节。len:{data_len},doc:{doc}")

            # 写入头长度
            buffer.write(struct.pack('h', data_len))

            for b in bs:
                buffer.write(struct.pack('b', b))

            # 如果块不够大，补齐块，占位
            val = chunk - data_len - self.DOC_HEADER_LEN
            for i in range(0, val):
                buffer.write(struct.pack('b', 0))

            # 写入到文件
            file.write(buffer.getvalue())
            pos = file.tell()

        # 记录位置即可
        index = int(pos / chunk)

        # print(f"位置：{index}")
        return index

    def add_document(self, doc):
        """
        1. 分词
        2. 写入字典
        3. 写入文档
        :param doc:
        :return:
        """

        keywords = []
        for f in self.index_fields:
            if f in doc:
                r = jieba.cut_for_search(doc.get(f))
                for i in r:
                    if i not in keywords:
                        # 索引全部存大写
                        keywords.append(i.upper())

        return self._index_doc(keywords, doc)

    def update_document(self, _pk, doc):
        """
        更新索引
        :param _pk:
        :param doc:
        :return:
        """

        # 需要记录下id对应文档的位置

        pass

    def delete_document(self):
        pass

    def search(self, keyword, highlight=False, limit=10):
        """
        1. 搜索词典
        2. 读取文档
        3. 根据评分排序
        :param limit:
        :param highlight:
        :param keyword:
        :return:
        """

        # 不缺分大小写字母
        keyword = keyword.upper()
        # 分词
        keys = jieba.cut_for_search(keyword)

        # 这搜索还要考虑到score和排序等
        # 击中的关键词
        hit_keys, doc_pos = self._search_index(keys, highlight)

        # print(f'检索到的数据位置：{doc_pos}')

        # print(f'得分情况：{pos_scores}')

        return self._get_doc(doc_pos, hit_keys, highlight, limit)

    def _search_index(self, keys, highlight):
        hit_keys = []
        # 得分
        doc_pos = []
        doc_pos_scores = {}
        key_len = 0
        for key in keys:
            s = time.time() * 1000
            pos = self.indexs.get(key, None)
            print(f"耗时：{time.time() * 1000 - s}")
            if pos:
                key_len += 1
                if highlight:
                    hit_keys.append(key)

                for p in pos:
                    if p not in doc_pos:
                        doc_pos.append(p)
                        doc_pos_scores[p] = 1
                    else:
                        doc_pos_scores[p] += 1

        # doc_pos_scores 转为数组

        pos_kv = []
        for i in doc_pos_scores:
            pos_kv.append((doc_pos_scores.get(i), i))

        # 冒泡排序
        n = len(pos_kv)
        for i in range(n):
            for j in range(0, n - i - 1):
                if pos_kv[j][0] < pos_kv[j + 1][0]:
                    pos_kv[j], pos_kv[j + 1] = pos_kv[j + 1], pos_kv[j]

        # 推导式，取到索引位置号
        doc_pos = [(i[1], i[0] / key_len) for i in pos_kv]

        return hit_keys, doc_pos

    def _get_doc(self, doc_pos, hit_keys, highlight=False, limit=10):
        """
        从文档中读取符合条件的数据
        :param doc_pos:
        :param hit_keys:
        :param highlight:
        :param limit:
        :return:
        """
        if not os.path.exists(self.doc_file):
            raise FileNotFoundError(f"没有找到索引文件:{self.doc_file}，请将建立索引。")
        rs = []
        with open(self.doc_file, 'rb') as f:
            for pos, score in doc_pos:
                start = (pos - 1) * self.DOC_CHUNK
                f.seek(start, 0)

                # 先读取2字节
                data_len, = struct.unpack('h', f.read(self.DOC_HEADER_LEN))
                if data_len == 0:
                    continue
                # print(f'数据包长度：{data_len}')
                r = f.read(data_len)
                if len(rs) < limit:
                    data = json.loads(r.decode())
                    # 高亮处理
                    if highlight:
                        data = self._highlight(hit_keys, data)

                    rs.append({
                        'score': score,
                        'pos': pos,
                        'doc': data
                    })
                else:
                    break
        return rs

    def _highlight(self, hit_keys, data):

        """
        高亮处理
        """
        for key in self.index_fields:
            if key in data:
                val = data.get(key)
                # 只有字符串才做处理
                if isinstance(val, str):
                    for hit in hit_keys:
                        val = val.replace(hit, f'{self.PRE_TAG}{hit}{self.POST_TAG}')
                    data[key] = val
        return data


def time_me(fn):
    def _wrapper(*args, **kwargs):
        start = int(time.time() * 1000)
        fn(*args, **kwargs)
        end = int(time.time() * 1000)

        print(f"{fn.__name__}执行耗时：{end - start}ms")

    return _wrapper
