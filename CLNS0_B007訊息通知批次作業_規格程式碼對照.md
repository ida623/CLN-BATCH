# CLNS0_B007 訊息通知批次作業 — 規格書 對照 程式碼

對照範圍：
- 規格書：`CLNS0_B007訊息通知批次作業_規格.md`
- 程式碼：`CLNS0_B007訊息通知批次作業_code.md`（類別 `CLNS0B007MessageSendTasklet`，實作 Spring Batch `Tasklet`）

## 0. 總覽結論

這支 Tasklet 對應到規格書「伍、處理邏輯說明」中的 **三、執行批次** 整段，也就是 `execute()` 方法從解析參數開始到訊息發送、寫入 log 為止。

但規格書中屬於「批次生命週期控制」的三段邏輯，**在本支程式碼中完全沒有出現**：

1. 一、前置作業 — 檢查 `CLN_BATCH_CTL` 是否有 `JOB_STATUS=1`（執行中），若有則中止
2. 二、執行批次前置 — 新增/更新 `CLN_BATCH_CTL`（寫入開始時間、JOB_STATUS='1'）
3. 四、紀錄執行結束時間 / 五、例外處理 / 六、FINALLY — 更新 `CLN_BATCH_CTL` 的 `JOB_STATUS`（'9' 成功 / '0' 失敗）與 `JOB_END_TIME`

**推測**：這些屬於「單一批次執行不能重複跑」的共用機制，在 Spring Batch 架構下通常會抽到 `JobExecutionListener` / `StepExecutionListener` 或框架共用元件處理，而不是寫在每支業務 Tasklet 裡，所以沒有在這支類別中重複出現。若這段邏輯真的完全沒有實作，會是需要跟需求方/架構確認的重要缺口 — 也就是理論上可能發生同一支批次在還沒跑完時被重複觸發（規格書 5 分鐘一次的排程）。

---

## 1. 對照總表

| 規格書章節 | 規格內容 | 程式碼對應 | 狀態 |
| --- | --- | --- | --- |
| 一、前置作業 1-5（共用參數、startTime、startDate、isSuccess） | 設定共用變數 | `execute()` 內 `LocalDateTime now`、`defaultDate`、`defaultTime`（第 106-108 行） | 部分對應（無 `isSuccess`，因為沒有 FINALLY 段） |
| 一、前置作業 6（檢查是否執行中） | 查 `CLN_BATCH_CTL` JOB_STATUS=1 | 無 | **缺** |
| 二、執行批次前置（寫入 CLN_BATCH_CTL） | insert/update 執行控制檔 | 無 | **缺** |
| 三、執行批次 1（宣告參數 cntMap、statusFail/statusSuccess 等） | 宣告輸入/輸出件數、狀態碼 | 第 97-99 行 `cntMap`；`buildMessageLogVo()` 內硬編 `3`／`sendSms/sendEmail` 內硬編 `-1` | 對應（狀態碼直接寫死在程式中，非獨立常數） |
| 三、執行批次 2（解析 arg0，設定 inputDate/inputTime/SendType） | 依參數個數分支、檢核日期格式 | 第 101-147 行 | 對應，且多了 `inputTime` 格式檢核（見差異 #1） |
| 三、執行批次 3（近 6 個月搬移 History） | insert into History + delete Log | 第 149-158 行 | 對應 |
| 三、執行批次 4（處理本次訊息通知） | 查 `CLN_MESSAGE_SETTING` | 第 160-166 行，SQL 檔 `CLNS0B007_QueryMessageSetting_Q001.sql` | 對應 |
| 　4.a 確認假日 | 呼叫 LIP-B-COMBUSSDQ001 | `isInputDateHoliday()`（第 207-216 行）內呼叫 `validWorkingdayUtil.validWorkingday(...)` | 對應，但 API 電文組裝被封裝在 `ValidWorkingdayUtil` 內，此檔看不到 | 
| 　4.b for 迴圈跑每個 messageSettingList | 逐一模板判斷時間窗與週期 | `processMessageSendSetting()`（第 221-341 行），由 `execute()` 第 188-194 行呼叫 | 對應 |
| 　　執行時間窗 / WRITE_HOLIDAY | if 條件式判斷是否要執行本模板 | 第 228-247 行 | 對應（邏輯等價，見說明） |
| 　　週期誤差 ≤1 分鐘 | `Math.abs((inputMM-writeStrMM)%CYCLE_MIN)<=1` | 第 249-255 行 | 對應 |
| 　　EXTRACTSTR 動態抽件 | 帶入 INPUT_DATE 參數後執行 SQL | 第 258-265 行 | 對應 |
| 　　逐筆組裝欄位、訊息替換 | MSG/SUBJECT/SEND_TYPE 等 | 第 269-276 行 `buildMessageLogVo()`（429-448 行） | 對應 |
| 　　資料檢核 chkMessageParams | 檢核失敗則 continue | 第 279-285 行 | 對應（多一個 errorMessage 非空判斷，見差異 #2） |
| 　　A: SMS / B,C: Email 分流組電文 | 組 smsMap / emailMap，塞入 index Map | 第 287-325 行 | 對應 |
| 　　未支援類型 | Log.fatal + 結束批次 | 第 326-329 行 `return false` | 對應（結束方式不同，見差異 #3） |
| 　　200 筆或最後一筆時批次送出 | 呼叫簡訊/Email API、寫 log、清空清單 | `flushMessageBatch()`（第 346-368 行） | **邏輯有出入，見差異 #4** |
| 　　呼叫 LIP-C-CMSSDSMST001（簡訊） | 上行組裝、失敗逐筆比對更新狀態 | `sendSms()`（第 373-397 行） | 對應 |
| 　　呼叫 LIP-C-CMSSMAILT001（Email） | 上行組裝、整批失敗更新狀態 | `sendEmail()`（第 402-424 行） | 對應 |
| 　　新增 log 訊息通知紀錄 | `CommonUtil.addMessageLog` | `saveMessageLogs()`（第 453-476 行）由 `flushMessageBatch()` 呼叫 | 對應 |
| 四、紀錄執行結束時間 | Log.info 結束時間 | 無獨立 log；只有 `logBatchCount()` 印輸入/輸出件數（第 496-498 行） | **缺**（結束時間 log 未見） |
| 五、例外處理（isSuccess=false） | try/catch 設定失敗旗標 | 無 | **缺** |
| 六、FINALLY（更新 CLN_BATCH_CTL 結束狀態） | update JOB_STATUS/JOB_END_TIME | 無 | **缺** |

---

## 2. 逐步詳細對照

### 2.1 參數解析（規格 三-2 → code 101-147 行）

- 無參數：`inputDate=startDate`、`inputTime=HHmm`、`SendType=""`，對應規格 a 情境。
- 1 個參數：驗證日期格式（`LocalDate.parse` + `BASIC_ISO_DATE`），失敗則 `log.error` 後 `return RepeatStatus.FINISHED`，對應規格「傳入參數非日期格式」+「結束批次」。
- 2 個參數：`inputTime=arg0[1]`，`SendType=""`。
- 3 個參數：`inputTime=arg0[1]`，`SendType=arg0[2]`。
- 其他長度：`log.error("傳入參數個數有誤")` 後結束。
- **多出規格沒有明寫的部分**：第 139-145 行額外對 `inputTime` 做 `LocalTime.parse(..., "HHmm")` 格式驗證，失敗一樣結束批次。規格書文字沒有描述這段檢核，屬於程式端額外補強（見差異 #1）。

### 2.2 六個月資料歸檔（規格 三-3 → code 149-158 行）

- `inputDateS6M = inputDate - 6 個月`，對應規格 `InputDateS6M`。
- 先執行 `SQL_I001`（insert into CLN_MESSAGE_HISTORY select from CLN_MESSAGE_LOG where INPUT_TIME <= ... and NEWS_STATUS='3'），對應規格搬移新增。
- 再呼叫 `clnMessageLogRepository.deleteByInputTimeLessThanEqualAndNewsStatus(cutoff, 3)` 做刪除，對應規格 delete 段。
- 規格書用同一個 Transaction 包住 insert+delete；程式碼看不到明確的 transaction 邊界（可能由 Spring 的 `@Transactional` 或框架層包住，此檔案未標註）。

### 2.3 查詢訊息來源設定（規格 三-4.1 → code 160-166 行）

- `SQL_Q001` 帶入 `InputDate`、`SendType`（`StringUtils.trimToNull` 後傳入，空字串會變成 `null`，對應規格「@SendType 不為空才加入 SQL」的條件式 SQL 寫法）。

### 2.4 假日判斷（規格 三-4.2 → code 180-186、207-216 行）

- 規格書描述了組 `BaseDate`（`02-3月-26` 格式）並呼叫 LIP-B-COMBUSSDQ001 API 取得 `WorkingDayFlag`。
- 程式碼把這段完全封裝進 `validWorkingdayUtil.validWorkingday(WorkingDayEnum.WORKING_DAY.getBussDateCheckMode(), inputDateTime)`，日期格式轉換與 API 呼叫細節都在這個共用元件內，本檔案看不到。
- `isHoliday = !isWorkingDay`，與規格 `isHoliday = WorkingDayFlag=='N'` 邏輯等價。
- 呼叫失敗時（`catch Exception`）回傳 `Optional.empty()`，外層直接結束批次（第 182-185 行），規格書對這個失敗情境沒有明確描述，屬程式端補強的容錯處理。

### 2.5 逐模板判斷是否執行（規格 三-4.2.b 內層 if → code 225-255 行）

規格條件式：
```
if ((!isHoliday || "Y".equals(WRITE_HOLIDAY)) && StringUtils.isNotEmpty(WRITE_STR_TIME) && StringUtils.isNotEmpty(WRITE_END_TIME)) {
  if (inputTime >= writeStrTime && inputTime <= writeEndTime) {
    if (Math.abs((inputMM - writeStrMM) % CYCLE_MIN) <= 1) { ... 執行 ... }
  }
}
```

程式碼改寫成「提早 return（跳過本模板）」的寫法，數學上等價：

```java
if (StringUtils.isAnyBlank(writeStrTime, writeEndTime)) return true;      // 對應 isNotEmpty 判斷
if (cycleMin <= 0) return true;                                          // 規格沒有此檢核，屬額外防呆
if ((isHoliday && !"Y".equals(writeHoliday)) || currentInputTime < writeStart || currentInputTime > writeEnd) return true;
if (Math.abs((inputMinute - writeStartMinute) % cycleMin) > 1) return true;
```

`(isHoliday && !"Y".equals(writeHoliday))` 是 `(!isHoliday || "Y".equals(WRITE_HOLIDAY))` 的邏輯反相，兩者等價（De Morgan），確認無誤。

### 2.6 逐筆組裝與檢核（規格 三-4.2.b.v-viii → code 258-329 行）

- `EXTRACTSTR` 動態 SQL：`messageUtil.replaceParam(extractStr, {InputDate})` 後查詢，對應規格「加工 EXTRACTSTR」。
- `cntMap["inputcnt"]` 累加 `extractstrList.size()`，對應規格輸入件數統計。
- 每筆呼叫 `messageUtil.chkMessageParams(messageLog)`：規格 `if(isContinue=='Y'){ log; 跳過本筆 }`；程式碼多加了 `StringUtils.isNotBlank(errorMessage)` 的條件（第 282 行），也就是 `isContinue=='Y'` 但 `errorMessage` 為空時，程式碼不會跳過，會直接往下處理該筆。這與規格書字面描述有出入（見差異 #2）。
- `notifyKind`：取 `SendType` 第一碼，`A`→SMS、`B`/`C`→Email，其餘 → `log.error` + `return false`（讓整個模板迴圈提前結束、外層 `execute()` 直接進入 `logBatchCount()` 並 `FINISHED`），對應規格「未支援類型 → 噴錯結束批次」，只是用回傳 `false` 代替丟例外（見差異 #3）。

### 2.7 分批送出（規格 三-4.2.b.viii.11-13 → code 331-368 行）

規格：
```
messageList.add(extractstrMap);
if (messageList不為空 && (messageList >= 200筆 or 本次迴圈已經是extractstrList最後一筆了)) {
  // 呼叫 SMS/Email API、寫 log、清空
}
```

程式碼（第 331-337 行）：
```java
messageList.add(messageLog);
if (messageList.size() >= 200) {
  flushMessageBatch(messageList, messageLogBySendKey, cntMap, smsList, emailList);
}
```

只保留了「滿 200 筆」的觸發條件，**沒有**「本次迴圈已經是 extractstrList 最後一筆」這個觸發條件。最終的 `flushMessageBatch()` 呼叫是放在 `execute()` 第 196 行，也就是**所有 messageSettingList 模板都跑完之後**才統一補送一次，而不是規格書所描述的「每個模板抽件跑完就送一次」。

實務結果差異：所有資料最終都會被送出、寫 log，件數不會少；差別在於**送出的時間點與批次的分界**——例如模板 A 抽出 50 筆、模板 B 抽出 30 筆，規格書預期是 A 跑完先送 50 筆一批、B 跑完再送 30 筆一批；程式碼則是等 A+B 都跑完，累積到 80 筆才一次送出（除非中途某個模板剛好讓累積數 ≥200 才會提前送）。如果下游 API 或關聯資料有「同一批模板」的隱含假設，這裡要跟熟悉業務的人確認是否影響（見差異 #4）。

### 2.8 呼叫簡訊 / Email API（規格 三-4.2.b.viii.11.a/c → code 373-424 行）

- `sendSms()`：組 `CMSSDSMST001Tranrq`（`ModuleTypeCode='CLN'`），呼叫後若整體 `RETURNCODE != '0000'`，逐筆比對 `SMSlNotifyItemTranrs` 的 `ReturnDesc != "Success"`，用 `buildSmsKey(applyId, notifyType, mobile)`（對應規格 3-key `APPLY_ID/NOTIFY_TYPE/RecipientCellPhone`）回查 `messageLogBySendKey`，設 `NEWS_STATUS=-1`。與規格完全對應。
- `sendEmail()`：組 `CMSSMAILT001Tranrq`，若整體 `RETURNCODE != '0000'`，**全部** `emailList` 標記失敗（規格也是整批失敗處理，無逐筆結果），用 `buildEmailKey(applyId, subject, email)` 回查。對應規格。

### 2.9 寫入訊息紀錄 / 結束（規格 三-4.2.b.viii.11.e、四 → code 361-367、453-476 行）

- `saveMessageLogs()` 把 `ClnMessageLogVo` 轉成 `ClnMessageLogEntity` 後 `saveAll`，對應規格 `CommonUtil.addMessageLog(messageList)`。
- `cntMap["outputcnt"]` 累加，對應規格輸出件數統計。
- `logBatchCount()`（第 496-498 行）只印「輸入件數/輸出件數」，規格書「紀錄執行結束時間」的 log（含時間戳）在此檔案中沒有對應輸出。

---

## 3. API 對照

| 規格書 API | 呼叫位置 | 說明 |
| --- | --- | --- |
| LIP-B-COMBUSSDQ001 查詢營業日 | `isInputDateHoliday()` → `validWorkingdayUtil.validWorkingday(...)` | 電文組裝細節封裝在 `ValidWorkingdayUtil`，本檔案看不到上下行欄位對應 |
| LIP-C-CMSSDSMST001 簡訊發送服務 | `sendSms()`（373-397 行） | `tranrq.setModuleTypeCode("CLN")`、`setSmsNotifyList(smsList)`，透過 `normalReqFactory.genRequestTemplate(...)` 組 MWHEADER（`sourceChannel`＝`cub.ap-id` 設定值，對應規格 `SOURCECHANNEL`；`txnSeq`＝Job Parameter） |
| LIP-C-CMSSMAILT001 行內信寄送 | `sendEmail()`（402-424 行） | 同上組法，`SenderEmail`/`SenderName` 未見程式碼中顯式設定（規格允許空值，交由中台帶預設寄件人），與規格「SenderEmail 為空則以模組代碼取得預設寄件人」一致 |

---

## 4. 差異與待確認事項彙整

1. **inputTime 格式檢核**：程式碼多做了 `LocalTime.parse(inputTime, "HHmm")` 驗證（第 139-145 行），規格書文字未明確描述此檢核，但屬合理補強，不影響既有行為。
2. **chkMessageParams 判斷條件**：規格「`isContinue=='Y'` 就跳過」，程式碼是「`isContinue=='Y'` **且** `errorMessage` 非空才跳過」（第 282 行）。若 `chkMessageParams` 真的會回傳 `isContinue='Y'` 但 `errorMessage` 為空的情境，兩者行為會不同，建議跟 `MessageUtil.chkMessageParams` 的實作或原開發者確認这个條件是否為刻意設計。
3. **未支援類型的結束方式**：規格是「噴錯（丟例外）結束批次」，程式碼是 `log.error` + `return false`，經由呼叫鏈讓 `execute()` 提前 `return RepeatStatus.FINISHED`。最終效果都是「批次不再繼續」，但程式碼是「正常結束」而非「拋例外中斷」，若外層有依賴 Step 失敗狀態（如 JobStatus=FAILED）做告警，這裡可能不會觸發，需要確認是否符合預期。
4. **200 筆分批送出時機**：規格是「滿 200 筆 **或** 該模板抽件跑完最後一筆」就送出一批；程式碼只在「滿 200 筆」時送出，其餘所有模板處理完後才在 `execute()` 最外層統一送出剩餘的。件數最終正確，但批次分界與規格描述不同，需確認業務面是否要求「同一模板一批送出」。
5. **CLN_BATCH_CTL 批次控制邏輯整段缺席**：規格書「一、前置作業-檢查執行中」「二、執行批次前置-寫入 CTL」「六、FINALLY-更新 CTL 結束狀態」在本 Tasklet 中完全沒有實作，也沒有看到呼叫其他共用元件的痕跡。這是本次對照中風險最高的落差，建議確認：
   - 是否由 Job 層的 Listener（例如某個共用 `JobExecutionListener`）處理，只是不在這支檔案裡；
   - 若真的沒有實作，代表「防止批次重複執行」與「JOB_STATUS 成功/失敗紀錄」目前是缺失的，5 分鐘一次的排程有機會在前一輪還沒跑完時就被下一輪觸發。
6. **「紀錄執行開始/結束時間」的 Log**：規格書要求在開始與結束都各印一行含時間戳的 log，程式碼目前只有 `logBatchCount()` 印輸入/輸出件數，沒有看到對應的開始/結束時間 log（可能同樣併在缺失的批次控制邏輯裡）。

---

## 5. 小結

程式碼忠實實作了規格書「執行批次」核心的業務邏輯（參數解析、資料歸檔、假日判斷、逐模板時間窗與週期判斷、簡訊/Email 組電文與逐筆/整批失敗回寫），邏輯等價性都有驗證過。主要落差集中在：**批次生命週期控制（CLN_BATCH_CTL）完全沒出現**，以及**分批送出的觸發時機**與規格文字描述不同。建議優先確認第 4 節的第 5 點（CTL 控制邏輯），這是唯一可能造成實際運維風險（重複執行）的落差；其餘幾點多為程式碼實作方式與規格偽代碼描述方式不同，語意上大致等價。
