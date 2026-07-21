# CLNS0_B007 訊息通知批次作業 — v1.0 → v1.1 差異表

比對範圍：
- 舊版：`CLNS0_B007訊息通知批次作業_規格.md`（v1.0，2026.03.19）
- 新版：`CLNS0_B007訊息通知批次作業_新版規格.md`（v1.1，2026.07.13）

修訂歷程原文（新版第 65-71 行）：

| 版本 | 日期 | 修訂說明 | 編修人員 |
| --- | --- | --- | --- |
| 1.0 | 2026.03.19 | 初版 | Nick |
| 1.1 | 2026.07.13 | 發送通知前，先新增log資料表 | Nick |

---

## 1. 差異總表

| # | 類別 | v1.0 | v1.1 | 性質 |
| --- | --- | --- | --- | --- |
| 1 | 核心流程 | 寄送 SMS/Email 完成後才 insert 一次 `CLN_MESSAGE_LOG` | 寄送前先 insert（狀態=2 待發送）→ 呼叫 API → 再 update 狀態 | **功能性變更（本次修訂主旨）** |
| 2 | 基本資訊 | URI：`/common/batch/messageSend` | URI：`/commonEntrance` | 功能性變更，待確認是否刻意 |
| 3 | 欄位 | 無 `SEND_COUNT`、`INPUT_TIME` 賦值 | 新增 `SEND_COUNT=1`、`INPUT_TIME=系統時間` 賦值 | 功能性新增（配合#1） |
| 4 | Update Key | 無此段落 | 新增 update 用 key：`APPLY_ID+SEND_TYPE+INFM_ID+INPUT_TIME+[MEMO_KEY]` | 功能性新增（配合#1） |
| 5 | Email 收件人組裝 | `recipientMap.put(...)` ×3 | 誤植為 `emailMap.put(...)` ×3（`emailMap` 此時尚未宣告） | **新增 bug** |
| 6 | 命名/變數修正 | `CommonUtil.replaceParam` / `CommonUtil.chkMessageParams` | 改為 `MessageUtil.replaceParam` / `MessageUtil.chkMessageParams` | 文字修正（貼近程式碼實際類別名） |
| 7 | 命名/變數修正 | `paramMap.put("INPUT_DATE", ...)`（變數名與宣告的 `extractstrParamMap` 不一致） | 改為 `extractstrParamMap.put("InputDate", ...)` | 文字修正 |
| 8 | 命名/變數修正 | `extractstrMapByKey.put(APPLY_ID, NOTIFY_TYPE, RecipientCellPhone, extractstrMap)`（`RecipientCellPhone` 未定義） | 改為 `extractstrMapByKey.put(APPLY_ID, NOTIFY_TYPE, MOBILE, extractstrMap)` | 文字修正 |
| 9 | API 規格文件 | LIP-C-CMSSDSMST001 下行電文表尾有 1 行多餘的 `ModuleTypeCode` 殘影 | 殘影從 1 組變成 3 組（`ModuleTypeCode`/`SMSlNotifyItemTranrs`/`SMSlNotifyNbr` 重複貼了 3 次） | 文件排版瑕疵，v1.1 惡化 |

---

## 2. 逐項詳細差異

### 2.1（核心變更）log 寫入時機：先寄送再寫 log → 先寫 log 再寄送

**v1.0**（規格.md 第 349-414 行）：
```
messageList.add(extractstrMap);
If(messageList不為空 && (messageList >= 200筆 or 最後一筆)){
  If(smsList不為空){ 呼叫SMS API ... smsList清空; }
  If(emailList不為空){ 呼叫Email API ... emailList清空; }
  //新增log訊息通知紀錄
  CommonUtil.addMessageLog(messageList);
  cntMap.put("outputcnt", ... + messageList.count);
  messageList清空;
}
```

**v1.1**（新版規格.md 第 349-485 行）：
```
messageList.add(extractstrMap);
If(messageList不為空 && (messageList >= 200筆 or 最後一筆)){
  //自行新增log訊息通知紀錄 NEWS_STATUS = 2:待發送/處理中
  Transaction.begin()
  For(messageMap : messageList){ 新增資料到CLN_MESSAGE_LOG（NEWS_STATUS=2, SEND_COUNT=0, 無OUTPUT_TIME）}
  Transaction.commit();

  If(smsList不為空){ 呼叫SMS API ... smsList清空; }
  If(emailList不為空){ 呼叫Email API ... emailList清空; }

  //更新log訊息通知紀錄
  Transaction.begin()
  For(messageMap : messageList){
    update CLN_MESSAGE_LOG
    set NEWS_STATUS = messageMap.NEWS_STATUS,
        SEND_COUNT = messageMap.SEND_COUNT,
        OUTPUT_TIME = 系統時間
    where APPLY_ID = messageMap.APPLY_ID
      and SEND_TYPE = messageMap.SEND_TYPE
      and INFM_ID = messageMap.INFM_ID
      and INPUT_TIME = messageMap.INPUT_TIME
      [and MEMO_KEY = messageMap.MEMO_KEY]
  }
  Transaction.commit();
  cntMap.put("outputcnt", ... + messageList.count);
  messageList清空;
}
```

**影響**：
- 三段式改成三個獨立 Transaction（insert-commit → 呼叫外部 API（不包 transaction）→ update-commit），避免 DB transaction 在等外部 API 回應時被長時間佔用，這部分設計合理。
- 但也帶來新風險：**insert 完成後、update 執行前若程式當掉或拋例外，該批紀錄會永久卡在 `NEWS_STATUS='2'`（待發送/處理中）**，規格書沒有描述任何補救/監控機制。這是本次修訂最需要跟需求方確認的地方。
- 目前程式碼（`CLNS0_B007訊息通知批次作業_code.md`）完全沒有實作這個「先 insert 再 update」的兩階段寫法，只有寄送後一次性 `saveMessageLogs()`（第 361-367、453-476 行），這段需要重新開發。

### 2.2 API URI 變更

- v1.0（規格.md 第 77 行）：`URI：/common/batch/messageSend`
- v1.1（新版規格.md 第 77 行）：`URI：/commonEntrance`

看起來不像批次專屬路徑命名，建議確認是刻意改為共用進入點設計，還是編修時誤貼了別份文件的內容。

### 2.3 新增欄位賦值：SEND_COUNT、INPUT_TIME

v1.1 在逐筆處理區塊新增兩行（新版規格.md 第 301-302 行）：
```
extractstrMap.put("SEND_COUNT",1);// 發送次數，更新成功失敗紀錄表才使用該參數，此處皆為初次發送故為1
extractstrMap.put("INPUT_TIME",系統時間);寫入時間 //直接定義寫入時間供後續更新紀錄表使用
```

v1.0 沒有這兩行，也沒有把 `INPUT_TIME` 定義為「系統時間」。

**待確認事項**：目前程式碼的 `INPUT_TIME` 其實是「執行日期參數 + 執行時間參數」組出來的批次業務時間（`CLNS0B007MessageSendTasklet.java` 第 241-242 行 `inputTimestamp`），並非真正的 wall-clock 系統時間。若照 v1.1 改成系統時間：
- 手動補跑（帶入歷史 `inputDate` 參數）時，`INPUT_TIME` 會變成「現在」而不是「補跑的那個業務日期」；
- 這會跟既有的六個月歸檔判斷條件（`INPUT_TIME <= to_date(InputDateS6M,'YYYYMMDD')`）的語意基準對不齊。

需要跟需求方確認 `INPUT_TIME` 到底該記錄「業務執行時間」還是「實際寫入的系統時間」——這是語意變更，不是單純措辭調整。

### 2.4 新增 Update 比對 Key

v1.1 的 update where 條件用 `APPLY_ID + SEND_TYPE + INFM_ID + INPUT_TIME + [MEMO_KEY]`（新版規格.md 第 474-478 行）當唯一鍵，v1.0 完全沒有這段（因為 v1.0 沒有 update 動作）。

**待確認**：這組複合鍵沒有搭配資料表自帶的序號/PK，本質上偏脆弱——理論上同一人同一秒收到同一種 `SEND_TYPE` 的兩筆通知時，`INPUT_TIME`（系統時間，精度可能到秒或毫秒）有機會撞在一起，導致 update 條件一次比對到不只一筆。建議 insert 時把資料表自動產生的序號存回記憶體，update 改用序號比對會更保險。

### 2.5【新增 bug】Email 收件人組裝變數誤植

**v1.0**（規格.md 第 322-327 行，正確）：
```java
List<Map> recipientList = new ArrayList<Map>();
Map recipientMap = new HashMap();
recipientMap.put("RecipientEmail",EMAIL);
recipientMap.put("RecipientName",INFM_NAME);
recipientMap.put("CcFlag",'N');
recipientList.add(recipientMap);
```

**v1.1**（新版規格.md 第 324-329 行，錯誤）：
```java
List<Map> recipientList = new ArrayList<Map>();
Map recipientMap = new HashMap();
emailMap.put("RecipientEmail",EMAIL);      // 應為 recipientMap.put
emailMap.put("RecipientName",INFM_NAME);   // 應為 recipientMap.put
emailMap.put("CcFlag",'N');                // 應為 recipientMap.put
recipientList.add(recipientMap);
```

`emailMap` 在這個時間點根本還沒宣告（要到新版規格.md 第 336 行才 `Map emailMap = new HashMap();`）。照這段偽代碼實作，`recipientMap` 會是空的，`recipientList` 加入的收件人物件會缺 `RecipientEmail`/`RecipientName`/`CcFlag` 三個欄位，等於行內信 API 收不到正確的收件人資訊。**這是 v1.1 才引入的錯誤，v1.0 原本是對的，需要修正回 `recipientMap.put(...)`。**

### 2.6 命名一致性修正（v1.1 修得比較正確，非缺陷）

| 項目 | v1.0 | v1.1 |
| --- | --- | --- |
| 抽件參數 Map | `Map extractstrParamMap = new HashMap(); paramMap.put("INPUT_DATE", @inputDate);`（變數名不一致） | `Map extractstrParamMap = new HashMap(); extractstrParamMap.put("InputDate", @inputDate);` |
| 樣板取代工具類別 | `CommonUtil.replaceParam(...)` | `MessageUtil.replaceParam(...)` |
| 訊息檢核工具類別 | `CommonUtil.chkMessageParams(...)` | `MessageUtil.chkMessageParams(...)` |
| SMS index key 第三欄 | `extractstrMapByKey.put(APPLY_ID, NOTIFY_TYPE, RecipientCellPhone, extractstrMap)`（`RecipientCellPhone` 未定義的變數） | `extractstrMapByKey.put(APPLY_ID, NOTIFY_TYPE, MOBILE, extractstrMap)` |

這幾處 v1.1 的寫法跟目前程式碼實際使用的類別/變數名稱（`MessageUtil`、`MOBILE`）更一致，屬於文件品質改善，不算功能性差異。

### 2.7 API 規格文件排版瑕疵（惡化）

LIP-C-CMSSDSMST001 下行電文表格（陸、API規格）：

- v1.0（規格.md 第 552-562 行）結尾已經有 1 行多餘的 `ModuleTypeCode` 殘影（貼錯留下的孤兒列）。
- v1.1（新版規格.md 第 624-638 行）在同樣位置又重複貼了兩組完整的 `ModuleTypeCode`/`SMSlNotifyItemTranrs`/`SMSlNotifyNbr`，變成總共 3 組類似內容疊在一起。

純屬文件表格複製貼上遺留的瑕疵，不影響程式邏輯，但建議一併清理，以免對外交付時造成閱讀者誤解欄位結構。

---

## 3. 待確認/待修正清單（依優先度排序）

1. **【必修】** Email 收件人組裝的 `emailMap.put(...)` 應改回 `recipientMap.put(...)`（2.5）。
2. **【需確認】** insert（狀態=2）與 update 之間若程式中斷，卡住的紀錄要如何被發現與補救，規格未描述（2.1）。
3. **【需確認】** `INPUT_TIME` 語意是否真的要改成系統時間，會影響手動補跑與六個月歸檔邏輯（2.3）。
4. **【建議評估】** Update 比對 key 建議改用資料表序號/PK，避免用系統時間當複合鍵的邊界風險（2.4）。
5. **【需確認】** URI 從 `/common/batch/messageSend` 改成 `/commonEntrance` 是否為刻意設計（2.2）。
6. **【文件清理】** LIP-C-CMSSDSMST001 下行電文表格的重複列刪掉（2.7）。
