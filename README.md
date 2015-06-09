结构化存储PHP SDK
========================
1. 安装第三方依赖

```
./composer.phar install
```

2. php环境配置：
  需要安装Client URL Library（php curl）扩展,
否则会报PHP Fatal error:  Call to undefined function SDS\Client\curl_init()。

3. 客户端metrics默认为关闭，当开启时，
需要安装php pthreads扩展。

4. 运行示例代码，测试是否正常(需要修改示例代码中的AppID/AppSecret)

```
php examples/Basic.php
```


SDS PHP SDK User Guide
========================
1. Install dependencies

```
./composer.phar install
```

2. PHP Extensions:
* php curl extension (required)
* php pthreads extension (optional,required only when client metrics turned on)

3. Run examples (you need to change the AppID/AppSecret in the example code)

```
php examples/Basic.php
```