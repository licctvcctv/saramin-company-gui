# Saramin Company GUI

桌面化 Saramin 企业信息采集工具，字段固定为：

- 公司名
- 老板名
- 公司官网
- 源头链接

## 运行方式

```bash
python -m pip install -r requirements.txt
python launcher.py
```

默认会自动打开浏览器；如需手动打开：

```bash
python -m saramin_app.server --no-browser --port 19180
```

## 打包 EXE

本仓库自带 GitHub Actions：

- Workflow: `Build Saramin GUI`
- 触发：`push main` 或手动 `workflow_dispatch`
- 产物：`saramin-company-gui-windows`（内含 `saramin_company_gui.exe`）
