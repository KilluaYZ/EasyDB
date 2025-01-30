# EasyDB

EasyDB是一个数据库，支持大多数SQL语句。


# Getting Start

## 安装Nix，开启Flake

编译环境的构建使用了Nix Flake，所以在使用之前需要进行相应配置。运行以下命令即可安装Nix：

```shell
$ sh <(curl -L https://nixos.org/nix/install) --daemon
```

修改`/etc/nix/nix.conf`，在其中添加下面的内容，即可开启Flake

```
substituters = https://mirrors.tuna.tsinghua.edu.cn/nix-channels/store https://cache.nixos.org/
experimental-features = nix-command flakes
```

## 编译

运行以下命令行，无需进行任何环境配置，nix会自动下载该项目的依赖包，并进行编译。首次编译时间可能会比较长，请耐心等待。编译结果会放在项目目录下的`result`目录中。

```shell
nix build
```

`result`目录结构如下：

```
result
├── bin
│   ├── easydb_client  #easydb的CLI客户端
│   └── easydb_server  #easydb的服务端
└── test #测试文件编译得出的二进制
```

## 开发环境

运行以下命令，nix会开启一个新的shell，该shell已经配置好了能够编译该项目的环境，这时使用cmake的编译方式正常编译即可。 在此终端中使用`code`等命令开启vscode可以让vscode等IDE找到对应的环境。

```shell
nix develop
```

## 运行


### 使用CLI交互

先编译出easydb的服务端，然后执行以下代码运行：

```shell
./result/bin/easydb_server -p 8888 -d test.db
```

然后重新开一个终端，运行以下命令：

```shell
./result/bin/easydb_client -p 8888
```

### 使用Web GUI交互

先编译出easydb的服务端，然后执行以下代码运行：

```shell
./result/bin/easydb_server -p 8888 -d test.db -w 
```

然后运行以下命令激活开发环境：

```shell
nix develop .
```

最后启动web界面和python写的代理服务器：

其中一个终端：

```shell 
cd web_client
python ./proxy/proxy_server.py 
```


开启另一个终端：

```shell 
cd web_client
npm install
npm run dev
```

然后访问 http://localhost:2000/ 就可以打开web界面。
