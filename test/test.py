import json
import os

from core import LowSearch, time_me


@time_me
def test_index():
    path = os.path.join(os.path.dirname(__file__), "index")

    # index_fields 是指定要索引的字段，其他字段不指定则忽略
    low = LowSearch(index_dir=path, index_fields=['title', 'content'])

    with open(os.path.join(os.path.dirname(__file__), 'test.json'), 'r') as f:
        json_str = f.read()
        data = json.loads(json_str)
        for i in data:
            print(i)

            """
            这个文档的格式和mongodb一样，任意的json就可以，然后在初始化LowSearch的时候，指定需要索引的字段
            搜索的时候将会原样返回json，如果配置了高亮，将会处理字段的高亮显示
            """
            _pk = low.add_document(i)
            print(f'索引id:{_pk}')


@time_me
def test_search():
    path = os.path.join(os.path.dirname(__file__), "index")
    low = LowSearch(index_dir=path, index_fields=['title', 'content'])

    @time_me
    def func():
        low.PRE_TAG = '<span style="color:red">'
        low.POST_TAG = '</span>'
        r = low.search("上海电信", highlight=True)
        for o in r:
            print(o)

    for i in range(0, 1):
        func()


if __name__ == '__main__':
    # test_index()
    test_search()
