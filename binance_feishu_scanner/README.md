# Binance USDⓈ-M Futures → Feishu Signal Scanner

这是一个**只做行情监控、信号筛选和回测，不自动下单**的小项目。

## 1. 数据从哪里来

本项目默认使用币安 USDⓈ-M Futures 公共 REST API：

- `GET /fapi/v1/exchangeInfo`：读取正在交易的 USDT 永续合约。
- `GET /fapi/v1/ticker/24hr`：读取 24 小时行情，并按 `quoteVolume` 选流动性较高的币。
- `GET /fapi/v1/klines`：读取 30m K 线 OHLCV。

这些是公开市场数据，所以本扫描器**不需要 Binance API Key**。

## 2. 四个策略

### A. MACD 顶背离 + 假突破（SHORT）

把“涨完后 MACD 回落、盘整、价格再创新高后做空”的想法写得更严格：

1. 前高之前必须存在明显上涨；
2. 前高必须是已经确认的局部 swing high；
3. 中间必须有至少数根 K 线的盘整/回撤；
4. 当前价格只允许“边际新高”，不接受巨型真突破；
5. 当前 MACD 和柱体弱于前高；
6. 新高后必须出现拒绝，例如收回前高之下或明显上影/阴线。

这样做的目的是避免“看到背离就逆势空”。

### B. 裸 K 扫流动性 / 假突破

- SHORT：刺破近 N 根 K 的前高，但收盘重新回到前高下方，并有明显上影。
- LONG：刺破前低，但收盘重新回到前低上方，并有明显下影。

这是最适合先单独回测的纯价格行为方案之一。

### C. 突破 + 回踩确认

先突破近 22 根 K 的支撑/阻力，再在之后 1~3 根 K 线回踩旧结构并站稳/压回。

### D. 压缩 → 扩张

近几根 K 线平均振幅显著收窄后，用较大实体突破近端箱体。

## 3. 评级

`final_score = 87% 技术形态分 + 13% 24h流动性分`

评级：

- S: >= 90
- A: >= 82
- B: >= 72
- C: >= 62

注意：**评级不是胜率。** 真正的胜率、收益因子、最大回撤必须由你自己的回测得到。

## 4. 安装

建议 Python 3.11 或 3.12。

```bash
python -m venv .venv

# macOS / Linux
source .venv/bin/activate

# Windows PowerShell
# .\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

复制环境变量：

```bash
cp .env.example .env
```

Windows 也可以直接复制 `.env.example` 并改名为 `.env`。

## 5. 飞书

在飞书群中添加**自定义机器人**，复制 Webhook 到：

```env
FEISHU_WEBHOOK=...
```

如果机器人开启了“安全密钥/签名校验”，再填：

```env
FEISHU_SECRET=...
```

先测试：

```bash
python binance_feishu_scanner.py --test-feishu
```

## 6. 单次扫描

```bash
python binance_feishu_scanner.py --once
```

只在终端看结果、不推飞书：

```bash
python binance_feishu_scanner.py --once --no-push
```

## 7. 每半小时自动扫描

```bash
python binance_feishu_scanner.py
```

默认会：

- 启动时立即扫描一次；
- 以后每小时 `:01` 和 `:31` 扫描。
- 使用 :01/:31 而不是 :00/:30，是为了尽量确保上一根 30m K 已完全收盘。

如果你希望精确改时间，改 `.env`：

```env
SCHEDULE_MINUTES=2,32
```

## 8. 回测

示例：

```bash
python backtest.py --symbols BTCUSDT,ETHUSDT,SOLUSDT --days 180
```

更严格地测试：

```bash
python backtest.py \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT \
  --days 365 \
  --rr 2.0 \
  --fee-bps 4 \
  --slippage-bps 2
```

输出到 `backtest_output/`：

- `trades_*.csv`
- `summary_*.csv`

### 回测假设

- 信号出现在收盘后；
- 下一根 K 的开盘价才进入统计；
- 同一策略同一币种不同时持有多笔；
- 若一根 K 同时碰止损和止盈，保守按先止损；
- 手续费和滑点是参数，必须换成你真实环境；
- `2R` 只是统一比较策略用的基准，不代表它就是最佳止盈方式。

## 9. 第一轮建议怎么测

不要立刻混合策略。先分别做：

1. 只测“裸 K 扫流动性”；
2. 只测“突破回踩”；
3. 只测“MACD 顶背离”；
4. 只测“压缩扩张”。

每个策略至少拆开统计：

- BTC/ETH 与山寨币；
- 牛/熊/横盘行情；
- 30m、1h、4h；
- LONG / SHORT；
- 交易次数；
- 胜率；
- Profit Factor；
- Total R；
- Max Drawdown；
- 加入真实手续费和滑点后的结果。

不要只优化“胜率”，容易得到过拟合策略。

## 10. 下一步可以升级

等第一轮回测跑出来后，再考虑：

- 多周期确认（30m 信号 + 4h 市场结构）；
- BTC 大盘方向过滤；
- Funding Rate / Open Interest / Long-Short Ratio 作为二级评分；
- 不同币种分组使用不同参数；
- Walk-forward / out-of-sample；
- 参数网格搜索；
- SQLite 保存历史扫描记录；
- Web dashboard。

