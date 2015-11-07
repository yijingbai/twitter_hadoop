## Visualization
这个code 只能在server上运行才可以读本地文件。
可以在当前目录执行：

```
python -m SimpleHTTPServer 8888&
```

然后打开浏览器访问:
```
http://localhost:8888/graph.html
```

就可以看到内容了。

`data.csv`就是数据的内容。需要说明的是，输出文件的第一行需要写`source,target`作为开头才可以。
