# 跨主机部署指南

## .gitignore 配置说明

当前的 `.gitignore` 配置确保以下内容**不会被提交到 git**：

### 1. 实验数据
- `data/` - 所有实验运行产生的数据

### 2. Python 缓存
- `__pycache__/`
- `*.py[cod]`, `*$py.class`, `*.so`

### 3. 前端编译产物
- `server/static/react/app.js`
- `server/static/react/app.css`
- `server/static/react/index.html`

这些是 Vite 构建的输出文件，每次构建都会重新生成。

### 4. Node.js 依赖
- `node_modules/`

### 5. 构建临时文件
- `web/.vite/`
- `web/tsconfig.tsbuildinfo`

---

## 在新主机上部署

### 前提条件
- Python 3.10+
- Node.js 18+
- npm 或 yarn

### 步骤 1: 克隆代码

```bash
git clone <repository-url>
cd cephfs_perf_test
```

### 步骤 2: 安装 Python 依赖

```bash
pip install -r requirements.txt
```

### 步骤 3: 安装前端依赖并编译

```bash
cd web
npm install
npm run build
cd ..
```

这会在 `server/static/react/` 目录生成编译后的文件：
- `app.js`
- `app.css`
- `index.html`

### 步骤 4: 启动 Web 服务

```bash
python run_web.py
```

### 步骤 5: 访问 Web 界面

打开浏览器访问 `http://127.0.0.1:18080`

---

## 验证清单

### Git 跟踪的文件（源代码）
- ✅ `web/src/**/*.jsx` - React 组件源代码
- ✅ `web/src/**/*.css` - 样式文件
- ✅ `web/src/**/*.js` - JavaScript 工具库
- ✅ `web/package.json` - 前端依赖配置
- ✅ `web/vite.config.js` - Vite 构建配置
- ✅ `server/**/*.py` - Python 后端代码
- ✅ `conf/*.json` - 配置文件

### Git 忽略的文件（编译产物）
- ❌ `server/static/react/app.js` - Vite 构建输出
- ❌ `server/static/react/app.css` - Vite 构建输出
- ❌ `server/static/react/index.html` - Vite 构建输出
- ❌ `web/node_modules/` - npm 依赖
- ❌ `data/` - 实验数据

---

## 常见问题

### Q: 为什么 `server/static/react/` 被忽略？
A: 这是 Vite 的构建输出目录，类似于 Python 的 `__pycache__` 或 Java 的 `target/`。源代码在 `web/src/` 中，每次构建都会重新生成这些文件。

### Q: 如何确认文件是否正确跟踪？
A: 运行以下命令查看 git 跟踪的文件：
```bash
git ls-tree -r HEAD --name-only | grep web/
```

应该看到：
- `web/src/...` (源代码) ✅
- `web/package.json` ✅
- `web/vite.config.js` ✅
- **不**应该看到 `web/node_modules/` ❌
- **不**应该看到 `server/static/react/` ❌

### Q: 编译后 Web 界面无法加载？
A: 检查：
1. `server/static/react/` 目录是否存在
2. `app.js` 和 `app.css` 是否已生成
3. Flask 是否正确配置了 static 路径

### Q: 如何清理编译产物？
A: 删除编译产物，保留源代码：
```bash
rm -rf server/static/react/
rm -rf web/node_modules/
rm -rf web/.vite/
```

然后重新编译：
```bash
cd web && npm install && npm run build
```

---

## 快速部署脚本

### Linux/Mac

```bash
#!/bin/bash
set -e

# 克隆代码
git clone <repository-url>
cd cephfs_perf_test

# 安装 Python 依赖
pip install -r requirements.txt

# 安装前端依赖并编译
cd web
npm install
npm run build
cd ..

# 启动服务
python run_web.py
```

### Windows (PowerShell)

```powershell
# 克隆代码
git clone <repository-url>
cd cephfs_perf_test

# 安装 Python 依赖
pip install -r requirements.txt

# 安装前端依赖并编译
cd web
npm install
npm run build
cd ..

# 启动服务
python run_web.py
```
