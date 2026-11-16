# agent-office-runner

在自己電腦上執行你在 Agent Office 雇用的 worker。辦公室網址：**https://khlab-office.com**。

worker 用你自己的 Claude、`gh`、`git` 登入，程式碼和登入資料都不會離開你的電腦。別人看得到你的 worker 在做什麼，但只有你能在它的終端機輸入。

> 這個 repo 是自動產生的打包版，請不要直接修改；原始碼在辦公室的 repo（`src/server/runner/`），由 `deploy/runner/publish.sh` 發佈到這裡。

## 1. 準備

- macOS 或 Linux（Windows 還不支援）。
- 公司網路或 VPN。
- Node 20 以上、git。
- `gh` 已登入（`gh auth status`），看得到要工作的 repo。
- Claude Code 已登入（終端機打 `claude` 不會要你登入）；要用 Codex 的話 `codex` 也要已登入。
- 辦公室的**個人帳號**：打開管理員給你的邀請連結（`https://khlab-office.com/join#…`），自己設密碼。

```bash
curl https://khlab-office.com/api/health        # 應該印出 {"ok":true}
```

## 2. 安裝（一次）

```bash
gh repo clone ClarkeYeh/agent-office-runner ~/agent-office-runner
cd ~/agent-office-runner && npm ci
```

## 3. 啟動（一台電腦一個，讓它一直開著）

```bash
node ~/agent-office-runner/bin/agent-office-runner https://khlab-office.com --name 你的帳號
```

- 第一次會問密碼，之後不用再輸入。看到 `✓ connected` 就是連上了。
- 之後你在任何一層樓雇用的 worker 都在這台電腦上跑。第一次在某層樓雇用時，runner 會用你的 `gh` 把那個 repo clone 到 `~/agent-office/projects/<owner>/<repo>`；runner 只在這個資料夾裡工作。
- 關掉 runner 後，worker 還會在你電腦上跑最多 30 分鐘，期間重新啟動會接回來。
- 說明裡寫的 `agent-office runner` 就是這個指令。

## 4. 第一次雇用 worker 時的確認

打開那個 worker 的終端機：

- **Claude Code**：問「Do you trust this folder?」時選「Yes, I trust this folder」。
- **Codex**：選「Trust and continue」，再輸入 `/hooks` 核准辦公室的 hook 指令。

每台電腦、每個 repo 只需要做一次。

## 5. 不用做的事

- 辦公室選單裡的「🔐 Your sign-ins」**不要設定**：那會把你的登入存到伺服器上，runner 不需要。

## 更新

```bash
cd ~/agent-office-runner && git pull && npm ci
```

再重新啟動 runner。辦公室更新後如果 runner 連不上、說版本不合，就是該更新了。
