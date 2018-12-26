# clj.qiniu

A Clojure library for qiniu.com storage that wraps [qiniu java-sdk](https://github.com/qiniu/java-sdk).

![alt test coverage](https://raw.github.com/leancloud/clj.qiniu/master/coverage.png)

## Usage

Leiningen dependency:

```clojure
[cn.leancloud/clj.qiniu "0.2.1"]
```

require it in your namespace:

```clojure
(require '[clj.qiniu :as qiniu])
```

### 配置

```clojure
(qiniu/set-config! :access-key "your qiniu access key" :secret-key "your qiniu secret key")
```

其他选项：

* `user-agent`:  请求的HTTP user agent值，默认`Clojure/qiniu sdk`。
* `throw-exception?`: 错误的时候是否抛出异常，默认 false，替代地返回 `{:ok false}`加上错误信息。

### 生成uptoken

```clojure
(qiniu/uptoken bucket)
(qiniu/uptoken bucket
	:expires 3600
	:scope scope
	:callbackUrl "http://exmaple.com/callback"
	:insertOnly 1
	:detectMime 1)
```

更多选项直接看源码吧。

### 上传文件

`file`参数可以是任何可以通过`clojure.java.io/input-stream`转成输入流的对象，比如 File 对象、URL、Java Resource等：

```clojure
(qiniu/upload-bucket bucket key file)
```

结果：

```clojure
{:ok true :status 200 :hash "xxxxx" :key "yyyyy" :response …… }
```

### 生成下载链接

```clojure
(qiniu/public-download-url domain key)
(qiniu/private-download-url domain key)
(qiniu/private-download-url domain key :expires (* 24 3600))
```

### 查看文件属性

```clojure
(qiniu/stat bucket key)
```

返回：

```clojure
{:hash "xxxx" :putTime 14048865740735254 :mimeType "image/jpeg"
 :status 200
 :fsize 16098}
```

### 拷贝、移动文件

```clojure
(qiniu/copy src-bucket src-key dest-bucket dest-key)
(qiniu/move src-bucket src-key dest-bucket dest-key)
```

返回：

```clojure
{:ok true ……}
```

### 删除文件：

```clojure
(qiniu/delete bucket key)
```
返回：

```clojure
{:ok true ……}
```

### 批量操作

使用`with-batch`配合`exec`搞定：

```clojure
(use '[clj.qiniu :only [with-batch stat]])
(with-batch
  (stat bucket key1)
  (stat bucket key2)
  (stat bucket key3)
  (exec))
```

返回：

```clojure
{:ok true :status 200
 :results ({:fsize 123 :hash "xxxx" ……}
             {:fsize 234 :hash "yyy" ……}
			 {:fsize 345 :hash "zzz" ……}
			 )}
```

批量拷贝、移动和删除文件也是类似：

```clojure
(with-batch
  (delete bucket key1)
  (delete bucket key2)
  (delete bucket key3)
  (exec))
```

返回：

```clojure
{:ok true :status 200
 :results ({:ok true} {:ok true} {:ok true})}
```

但是请注意，`with-batch`里的操作必须是同一种类型，不能混合。

### 图片处理

获取图片信息和 EXIF 信息：

```clojure
(image-info url)
(image-exif url)
```

获取图片缩略图：

```clojure
(image-view url :width 100 :height 100 :mode 1 :format "png")
```

返回结果的`:response`值就是缩略图的二进制数据，可以存储为文件或者输出到网页。

### 批量获取 Bucket 下的文件

根据前缀`prefix`获取 Bucket 内匹配的文件列表，`bucket-file-seq`会返回一个`LazySeq`：

```clojure
(bucket-file-seq  bucket "<prefix>" :limit 32)
```

limit设定批量查询大小，默认 32。

### Bucket 统计
查询单月统计：

```clojure
(bucket-monthly-stats bucket "201407")
```

查询某个时间范围内的空间、流量或者 API 调用统计：

```clojure
(bucket-stats bucket "space" "20140701" "20140710")
(bucket-stats bucket "transfer" "20140701" "20140710")
(bucket-stats bucket "apicall" "20140701" "20140710")
```

### Bucket 管理

创建 Bucket:

创建和删除 bucket:

```clojure
(mk-bucket bucket)
(remove-bucket bucket)
```

发布到开放域名:

```clojure
(publish-bucket bucket "http://example.qiniudn.com")
```

将 bucket 私有或者设置为公开：

```clojure
(private-bucket bucket true)
(private-bucket bucket false)
```

### 持久化处理（音频视频）

如果需要对已保存在空间中的资源进行云处理并将结果[持久化](http://developer.qiniu.com/docs/v6/api/reference/fop/pfop/pfop.html#pfop-notification)，可以使用`pfop`方法：

```clj
(pfop "clj-qiniu"  (java.net.URLEncoder/encode "viva la vida.mp3")
                    "avthumb/m3u8/segtime/10/preset/audio_32k"
                    "http://example.com/persistentNotify"
                    :pipeline "dennis")
;; {:ok true, :results "544df4f97823de406816e673"}
```

返回的`results`就是`persistentId`，可以用来查询持久化处理状态：

```
(prefop-status "544df4f97823de406816e673")
```

### 获取 CDN 域名

```clojure
(domain-list bucket)
```

刷新 CDN 缓存（可能需要申请相关权限）：

```clojure
(refresh-bucket-cdn urls dirs)
```

绑定自定义域名（可能要申请相关权限）：

```clojure
(set-custom-domain! domain bucket cret key platform geo protocol)
;;; geo "chain" or "global"
;;; protocol "http" or "https"
```

上下线自定义域名：
```clojure
(online-custom-domain domain)
(offline-custom-domain domain)
```

删除自定义域名：
```clojure
(delete-custom-domain domain)
```


## 贡献者

* [xhh](https://github.com/xhh)
* [juvenn](https://github.com/juvenn)

## License

Copyright © 2014 killme2008

Distributed under the Eclipse Public License version 1.0
