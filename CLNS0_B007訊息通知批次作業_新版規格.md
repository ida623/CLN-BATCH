有擔貸款模組

CLNS0\_B007訊息通知批次作業

目錄

[目錄 1](#_Toc225243618)

[壹、 系統規格 2](#_Toc225243619)

[一、 基本資訊 2](#_Toc225243620)

[貳、 需求描述 2](#_Toc225243621)

[參、 需求類型 2](#_Toc225243622)

[一、 需求資訊 2](#_Toc225243623)

[肆、 資料表 2](#_Toc225243624)

[一、 資料庫主機資訊 2](#_Toc225243625)

[二、 資料表資訊 2](#_Toc225243626)

[伍、 處理邏輯說明 2](#_Toc225243627)

[一、 前置作業 2](#_Toc225243628)

[二、 執行批次前置 3](#_Toc225243629)

[三、 執行批次 3](#_Toc225243630)

[四、 紀錄執行結束時間 11](#_Toc225243631)

[五、 例外處理 11](#_Toc225243632)

[六、 FINALLY 11](#_Toc225243633)

[陸、 API規格 11](#_Toc225243634)

[一、 LIP-B-COMBUSSDQ001 查詢營業日 11](#_Toc225243635)

[1. API資訊 11](#_Toc225243636)

[2. 上行電文 11](#_Toc225243637)

[3. 下行電文 12](#_Toc225243638)

[二、 LIP-C-CMSSDSMST001簡訊發送服務 13](#_Toc225243639)

[1. API資訊 13](#_Toc225243640)

[2. 上行電文 13](#_Toc225243641)

[3. 下行電文 14](#_Toc225243642)

[三、 LIP-C-CMSSMAILT001 行內信寄送 15](#_Toc225243643)

[1. API資訊 15](#_Toc225243644)

[2. 上行電文 15](#_Toc225243645)

[3. 下行電文 17](#_Toc225243646)

修訂歷程

| | | | | |
| --- | --- | --- | --- | --- |
| 版本 | 日期 | 修訂說明 | 編修人員 | 審核人員 |
| 1.0 | 2026.03.19 | 初版 | Nick | |
| 1.1 | 2026.07.13 | 發送通知前，先新增log資料表 | Nick | |

1. 系統規格
2. 基本資訊
  1. AP ID：LON-LX-LIP-01
  2. 程式名稱：CLNS0\_B007
  3. URI：/commonEntrance
  4. 類型：Spring Batch
  5. 運行平台：Paas 平台
  6. TFS專案路徑：http://pialm01:8080/tfs/DefaultCollection/LONLXLIP01-LIP/\_git/lip-svc-cln-batch [對應的TFS Repo路徑／連結]
3. 需求描述

批次訊息通知。

1. 需求類型
2. 需求資訊
  1. □畫面 □服務 ■批次 □其他
  2. 執行時間/頻率：自動:每5分鐘
  3. 流程：
3. 資料表
4. 資料庫主機資訊
  1. 資料庫系統：Oracle
  2. 主機資訊：

| | | | | |
| --- | --- | --- | --- | --- |
| 環境別 | 主機 | 服務名稱 | DB帳號/密碼 | 壓密檔名 |
| UT | 88.8.232.209 | OLIPPLNSRV | LONLXLIP01CLN | |
| UAT | | | | |
| PROD | | | | |

1. 資料表資訊

| | | |
| --- | --- | --- |
| DB Schema | 資料表 | 說明 |
| LONLXLIP01CLN | CLN\_BATCH\_CTL | 批次執行控制檔 |
| LONLXLIP01CLN | CLN\_MESSAGE\_LOG | 訊息發送記錄檔 |
| LONLXLIP01CLN | CLN\_MESSAGE\_HISTORY | 訊息發送歷史檔 |
| LONLXLIP01CLN | CLN\_MESSAGE\_SETTING | 訊息來源控制檔 |
| | | |

1. 處理邏輯說明
2. 前置作業
  1. 設定共用參數:
  2. 外部傳入參數 String[] arg0
  3. Timestamp startTime = 系統時間
  4. String startDate = 系統時間yyyyMMdd
  5. boolean isSuccess = true;
  6. 檢查程式是否執行中

| |
| --- |
| select NVL(COUNT(1),0) as RUNNING from CLN\_BATCH\_CTL CBC where CBC.EXEC\_DATE = @startDateand CBC.JOB\_NAME = @程式名稱and CBC.JOB\_STATUS = 1 |

- 1. 若RUNNING > 0，則Log.info(@程式名稱 + “批次作業正在執行中，停止進行”)，並結束程式。
1. 執行批次前置
  1. 紀錄開始時間
  2. Log.info(@程式名稱 + “紀錄執行開始時間:” + @startTime)
  3. 新增/更新CLN\_BATCH\_CTL(批次執行控制檔)

 Transaction.begin();

- - 1. 查詢CLN\_BATCH\_CTL

| |
| --- |
| select \* from CLN\_BATCH\_CTL CBC where CBC.EXEC\_DATE = @startDate and CBC.JOB\_NAME = @程式名稱 |

- 1. 查無資料則新增；有資料則更新。

| | |
| --- | --- |
| 欄位 | 內容 |
| EXEC\_DATE | @startDate |
| JOB\_NAME | @程式名稱 |
| ARGS | @程式名稱 + ‘ ’ + @startDate + ‘ ’ + “SYSTEM” |
| JOB\_STATUS | ‘1’ |
| UPD\_ID | “SYSTEM” |
| JOB\_START\_TIME | @startTime |
| JOB\_END\_TIME | null |
| DATA\_DATE | @startDate |

- 1. Transaction.commit();
1. 執行批次
  1. 宣告參數
  2. final Map<String, Integer> cntMap = new HashMap<String, Integer>(); // 紀錄件數的MAP
  3. cntMap.put("inputcnt", 0); // 輸入件數
  4. cntMap.put("outputcnt", 0); // 輸出件數
  5. final String inputDate; // 執行日期 (參數1)
  6. final String inputTime; // 執行時間 HHmm (參數2)
  7. final String SendType; // 種類 (參數3)
  8. String statusFail = "-1";//查詢失敗
  9. String statusSuccess = "3";//查詢成功
  10. List<Map> messageList = new ArrayList<Map>();//收集訊息發送紀錄清單 最多兩百筆一次寫入
  11. extractstrMapByKey = new HashMap<>();//多key Map(最後一參數為value)當作messageList的index使用 ，主要是為了效能，實作方式可調整
  12. 設定參數
  13. if (arg0 == null || arg0.length == 0) {//一般自動批次走此情境，不帶參數，全部執行
  14. inputDate = startDate;
  15. inputTime = startTime時間取HHmm;
  16. SendType = ""; // 種類(全部)
  17. }else{//特殊情境，或手動補跑時會帶參數
  18. inputDate = arg0[0];
  19. // 檢核日期是否正確
  20. if (!DATE.isDate(inputDate)) {
  21. Log.fatal("傳入參數非日期格式 : " + inputDate);
  22. 結束批次
  23. }
  24. if (arg0 != null && arg0.length == 1) {
  25. inputTime = startTime時間取HHmm;
  26. SendType = ""; // 種類(全部)
  27. } else if (arg0 != null && arg0.length == 2) {
  28. inputTime = arg0[1];
  29. SendType = ""; // 種類(全部)
  30. } else if (arg0 != null && arg0.length == 3) {
  31. inputTime = arg0[1];
  32. SendType = arg0[2];//種類(指定)
  33. } else{
  34. Log.fatal("傳入參數個數有誤 : 傳入長度=" + arg0.length);
  35. 批次結束
  36. }
  37. }// if (arg0 == null || arg0.length == 0){}else 結尾
  38. CLN\_MESSAGE\_LOG(訊息發送記錄檔)只保留近6個月的檔案，超過的部份搬到CLN\_MESSAGE\_HISTORY(訊息發送歷史檔)
  39. String InputDateS6M = inputDate -6個月; 20260319 => 20250919
  40. Transaction.begin();
  41. 將CLN\_MESSAGE\_LOG(訊息發送記錄檔)六個月以上的資料新增至CLN\_MESSAGE\_HISTORY(訊息發送歷史檔)

| |
| --- |
| - 1. insert into CLN\_MESSAGE\_HISTORY
  2. (select \*
  3. from CLN\_MESSAGE\_LOG
  4. where INPUT\_TIME <= to\_date(@InputDateS6M,'YYYYMMDD')
  5. and NEWS\_STATUS = '3'
  6. )
 |

- 1. 將CLN\_MESSAGE\_LOG(訊息發送記錄檔)六個月以上的資料刪除

| |
| --- |
| - 1. delete
  2. from CLN\_MESSAGE\_LOG
  3. where INPUT\_TIME <= to\_date(@InputDateS6M,'YYYYMMDD')
  4. and NEWS\_STATUS = '3'
 |

- 1. Transaction.commit();
  2. 處理本次訊息通知
    - 1. 查詢CLN\_MESSAGE\_SETTING (訊息來源控制檔)取得本次處理訊息項目messageSettingList

| |
| --- |
| - 1. select \*
  2. from CLN\_MESSAGE\_SETTING
  3. where SOURCE\_KIND = '1' -- 批次
  4. and to\_date(@inputDate,'YYYYMMDD') between STR\_DATE and END\_DATE
  5. [and SEND\_TYPE = @SendType (@SendType不為空才加入SQL) ]
 |

- 1. 確認是否為假日
  2. String BaseDate = inputDate 轉格式 => "02-10月 -26" or "02-3月 -26"
  3. 20260319 => "02-3月 -26" 9碼
  4. 20261019 => "02-10月 -26" 10碼
  5. 呼叫【LIP-B-COMBUSSDQ001 查詢銀行營業日】API
  6. 傳入上行

| | | |
| --- | --- | --- |
| LVL | 欄位 | 內容 |
| 1 | BaseDate | @BaseDate |

- 1. boolean isHoliday = LIP-B-COMBUSSDQ001下行.WorkingDayFlag == 'N'; // true:假日 false:平日
  2. 邏輯處理
  3. If(messageSettingList 不為空){
  4. for (map: messageSettingList) {//執行每一個訊息通知模板 //無資料就不執行也不報錯
  5. // 目前執行時間落在本次簡訊發送起迄時間內
  6. // 計算目前時間(換算分鐘數) - 發送簡訊起始時間(換算分鐘數) 共經過多久時間，週期性批次
  7. String WRITE\_STR\_TIME = map.WRITE\_STR\_TIME; //簡訊抽件時間 可看作開始時間
  8. String WRITE\_END\_TIME = map.WRITE\_END\_TIME; //寄送簡訊時間 可看作結束時間
  9. String WRITE\_HOLIDAY = map.WRITE\_HOLIDAY; //假日是否執行抽件 可看作假日是否執行
  10. int CYCLE\_MIN = map.CYCLE\_MIN轉數字 // 執行週期(每幾分鐘執行一次)
  11. int inputTime = INPUT\_TIME轉數字;//1700 => 1700
  12. int writeStrTime = WRITE\_STR\_TIME轉數字; //1700 => 1700
  13. int writeEndTime = WRITE\_END\_TIME轉數字; //1700 => 1700
  14. int inputMM = inputTime / 100 \* 60 + inputTime % 100; // 目前時間(換算分鐘數)
  15. int writeStrMM = writeStrTime / 100 \* 60 + writeStrTime % 100; // 發送簡訊起始時間(換算分鐘數)
  16. // 平日一律執行，假日則需看table設定，再決定是否要執行
  17. if ((!isHoliday || "Y".equals(WRITE\_HOLIDAY)) && StringUtils.isNotEmpty(WRITE\_STR\_TIME) && StringUtils.isNotEmpty(WRITE\_END\_TIME)) {
  18. if (inputTime >= writeStrTime && inputTime <= writeEndTime) {//檢查現在時間是否在該模板執行期間
  19. if (Math.abs((inputMM - writeStrMM) % CYCLE\_MIN) <= 1) { //誤差在1分鐘以內都算合理範例
  20. String extractstr = "";
  21. if(map.EXTRACTSTR不為空){ //加工EXTRACTSTR抽件語法
  22. Map extractstrParamMap = new HashMap();
  23. extractstrParamMap.put("InputDate", @inputDate);
  24. extractstr = MessageUtil.replaceParam(map.EXTRACTSTR, extractstrParamMap);
  25. //執行SQL取得簡訊內容參數，取得每一個寄信對象資料extractstrList

| |
| --- |
| - 1. @extractstr
 |

- 1. }
  2. List<Map> smsList = new ArrayList<Map>();//收集簡訊清單，一次發送
  3. List<Map> emailList = new ArrayList<Map>();//收集email清單，一次發送
  4. cntMap.put("inputcnt", cntMap.get("inputcnt") + extractstrList.count);//紀錄輸入件數
  5. for(extractstrMap : extractstrList){//無資料就不執行也不報錯
  6. String SUBJECT = map.TITLE null補空字串;
  7. String MSG = map.MSG null補空字串;
  8. String SEND\_TYPE = map.SEND\_TYPE null補空字串;
  9. String notifyKind = SEND\_TYPE取第一碼; // TYPE的第一碼
  10. String MOBILE = extractstrMap.MOBILE null補空字串;
  11. String EMAIL = extractstrMap.EMAIL null補空字串;
  12. String APPLY\_ID = extractstrMap.APPLY\_ID null補空字串;
  13. String NOTIFY\_TYPE = extractstrMap.NOTIFY\_TYPE null補空字串;
  14. String MEMO\_KEY = extractstrMap.MEMO\_KEY null補空字串;
  15. String INFM\_ID = extractstrMap.INFM\_ID null補空字串; // 通知人員
  16. String INFM\_NAME = extractstrMap.INFM\_NAME null補空字串; // 通知姓名
  17. String REMIT\_BRANCH = extractstrMap.REMIT\_BRANCH null補空字串;// 帳務分行 sms API上行 【費用分攤單位費用單位代號】使用
  18. // 取得SQL中所有欄位名稱，FIND要寄送的簡訊內容所對應之名稱(如ID對應 #ID#)並取代
  19. Map msgParamMap = new HashMap();
  20. 把extractstrMap每一個欄位名稱跟值都塞入msgParamMap中
  21. string msg = "";
  22. msg = MessageUtil.replaceParam(MSG, msgParamMap);
  23. extractstrMap.put("SEND\_TYPE",SEND\_TYPE);
  24. extractstrMap.put("SUBJECT",SUBJECT);
  25. extractstrMap.put("NEWS",msg);
  26. extractstrMap.put("NOTIFY\_TYPE",NOTIFY\_TYPE);
  27. extractstrMap.put("NEWS\_STATUS", statusSuccess);//預設成功，更新成功失敗紀錄表才使用該參數
  28. extractstrMap.put("SEND\_COUNT",1);// 發送次數，更新成功失敗紀錄表才使用該參數，此處皆為初次發送故為1
  29. extractstrMap.put("INPUT\_TIME",系統時間);寫入時間 //直接定義寫入時間供後續更新紀錄表使用
  30. //資料檢核
  31. Map chkMap = MessageUtil.chkMessageParams(extractstrMap);
  32. String isContinue = chkMap.isContinue;
  33. String errorMessage = chkMap.errorMessage;
  34. If(isContinue == 'Y'){
  35. Log.info(errorMessage);
  36. 結束本筆extractstrMap，繼續跑下一筆extractstrMap
  37. }
  38. If(notifyKind == 'A')){ // A: SMS 簡訊
  39. Map smsMap = new HashMap();
  40. smsMap.put("CaseNbr",APPLY\_ID);//案件編號
  41. smsMap.put("SMSNotifyContent",msg);//通知內容
  42. smsMap.put("SMSNotifySubject",SUBJECT);//通知主旨
  43. smsMap.put("NotifyCode",NOTIFY\_TYPE);//對外發信通知種類代碼
  44. smsMap.put("RecipientCellPhone", MOBILE);//收件人手機
  45. smsMap.put("RecipientId",INFM\_ID);//收件者統編
  46. smsMap.put("RecipientName",INFM\_NAME);//收件人姓名
  47. smsMap.put("AcntDeptCode", REMIT\_BRANCH);//費用分攤單位費用單位代號
  48. extractstrMapByKey.put(APPLY\_ID, NOTIFY\_TYPE, MOBILE,extractstrMap);//index
  49. smsList.add(smsMap); 新增進sms清單，後續呼叫通知API使用
  50. }else if(notifyKind in('B','C')){// B:INTERNAL-EMAIL 內部郵件// C: EXTERNAL-EMAIL 外部郵件 現實際無C情境
  51. List<Map> recipientList = new ArrayList<Map>();
  52. Map recipientMap = new HashMap();
  53. emailMap.put("RecipientEmail",EMAIL);//收件人E-MAIL
  54. emailMap.put("RecipientName",INFM\_NAME);//收件人姓名
  55. emailMap.put("CcFlag",'N');// Y:副本/N:正本/B:密件副本
  56. recipientList.add(recipientMap);
  57. String mailNotifyCode = "";
  58. If(notifyKind == 'B'){ //1:行內2:行外
  59. mailNotifyCode = '1';
  60. }else{
  61. mailNotifyCode = '2';
  62. }
  63. emailMap.put("CaseNbr",APPLY\_ID);//案件編號
  64. emailMap.put("MailNotifyCode", mailNotifyCode);// 1:行內2:行外
  65. emailMap.put("MailNotifyContent",msg);//信件通知內容
  66. emailMap.put("MailNotifySubject",SUBJECT);//信件通知主旨
  67. emailMap.put("MailTypeCode",'2');//發信類型 1:純文字 2:MIME(html標籤) 3:模版發信 4:BillHunter
  68. emailMap.put("NotifyCode",NOTIFY\_TYPE);// 對外發信通知種類代碼
  69. emailMap.put("RecipientList",recipientList);//收件人資訊
  70. extractstrMapByKey.put(APPLY\_ID, SUBJECT, EMAIL,extractstrMap);//index
  71. emailList.add(emailMap);//新增進email清單，後續呼叫通知API使用
  72. }else{
  73. Log.fatal("未支援類型 [" + TYPE + "]");
  74. 噴錯 結束批次
  75. }
  76. messageList.add(extractstrMap);新增進通知清單，計算兩百筆使用
  77. If(messageList不為空 && (messageList >= 200筆 or 本次迴圈已經是extractstrList最後一筆了)){//分批處理
  78. //自行新增log訊息通知紀錄 NEWS\_STATUS = 2:待發送/處理中
  79. Transaction.begin()
  80. For(messageMap : messageList){
  81. 新增資料到CLN\_MESSAGE\_LOG(訊息發送記錄檔)

| | |
| --- | --- |
| - 1. 欄位
 | - 1. 內容值
 |
| - 1. APPLY\_ID
 | - 1. messageList.APPLY\_ID
 |
| - 1. SEND\_TYPE
 | - 1. messageList.SEND\_TYPE
 |
| - 1. NEWS\_STATUS
 | - 1. 2 //待發送/處理中
 |
| - 1. INFM\_ID
 | - 1. messageList.INFM\_ID
 |
| - 1. INFM\_NAME
 | - 1. messageList.INFM\_NAME
 |
| - 1. SUBJECT
 | - 1. messageList.SUBJECT
 |
| - 1. EMAIL
 | - 1. messageList.EMAIL
 |
| - 1. MOBILE
 | - 1. messageList.MOBILE
 |
| - 1. NEWS
 | - 1. messageList.NEWS
 |
| - 1. INPUT\_TIME
 | - 1. messageList.INPUT\_TIME
 |
| - 1. SEND\_COUNT
 | - 1. 0
 |
| - 1. NOTIFY\_TYPE
 | - 1. messageList.NOTIFY\_TYPE
 |
| - 1. MEMO\_KEY
 | - 1. messageList.MEMO\_KEY
 |

- 1. }
  2. Transaction.commit();
  3. If(smsList不為空){
  4. 呼叫【LIP-C-CMSSDSMST001簡訊發送服務】API
  5. 傳入上行

| | | |
| --- | --- | --- |
| LVL | 欄位 | 內容 |
| 1 | ModuleTypeCode | 'CLN' |
| 1 | SMSNotifyList | smsList |
| 2 | CaseNbr | smsList.CaseNbr |
| 2 | SMSNotifyContent | smsList.SMSNotifyContent |
| 2 | SMSNotifySubject | smsList.SMSNotifySubject |
| 2 | NotifyCode | smsList.NotifyCode |
| 2 | RecipientCellPhone | smsList.RecipientCellPhone |
| 2 | RecipientId | smsList.RecipientId |
| 2 | RecipientName | smsList.RecipientName |
| 2 | AcntDeptCode | smsList.AcntDeptCode |

- 1. //API結果情境:整批成功、整批失敗、部分成功失敗
  2. If(LIP-C-CMSSDSMST001下行.RETURNCODE != '0000'){//錯誤處理
  3. For(map : LIP-C-CMSSDSMST001下行.SMSlNotifyItemTranrs){
  4. If(map.ReturnDesc != 'Success'){
  5. Map extractstrMap = extractstrMapByKey.get(map.CaseNbr,map.NotifyCode,map.RecipientCellPhone);//index
  6. extractstrMap.put("NEWS\_STATUS", statusFail);//更新狀態為失敗
  7. }
  8. }
  9. } //If(LIP-C-CMSSDSMST001下行.RETURNCODE != '0000'){ 結尾
  10. smsList清空;
  11. }// If(smsList不為空){ 結尾
  12. If(emailList不為空){
  13. 呼叫【LIP-C-CMSSMAILT001 行內信寄送】API
  14. 傳入上行

| | | |
| --- | --- | --- |
| LVL | 欄位 | 內容 |
| 1 | ModuleTypeCode | 'CLN' |
| 1 | SenderEmail | null |
| 1 | SenderName | null |
| 1 | MailNotifyList | emailList |
| 2 | CaseNbr | emailList.CaseNbr |
| 2 | MailNotifyCode | emailList.MailNotifyCode |
| 2 | MailNotifyContent | emailList.MailNotifyContent |
| 2 | MailNotifySubject | emailList.MailNotifySubject |
| 2 | MailTypeCode | emailList.MailTypeCode |
| 2 | NotifyCode | emailList.NotifyCode |
| 2 | RecipientList | emailList.RecipientList |
| 3 | RecipientEmail | RecipientList.RecipientEmail |
| 3 | RecipientName | RecipientList.RecipientName |
| 3 | CcFlag | RecipientList.CcFlag |

- 1. //API結果情境:整批成功、整批失敗
  2. If(LIP-C-CMSSMAILT001下行.RETURNCODE != '0000'){//錯誤處理
  3. For(map : emailList){
  4. Map extractstrMap = extractstrMapByKey.get(map.CaseNbr,map.MailNotifySubject,map.RecipientEmail);//index
  5. extractstrMap.put("NEWS\_STATUS", statusFail);//更新狀態為失敗
  6. }
  7. } //If(LIP-C-CMSSDSMST001下行.RETURNCODE != '0000'){ 結尾
  8. emailList清空;
  9. }// If(emailList不為空){ 結尾
  10. //更新log訊息通知紀錄
  11. Transaction.begin()
  12. For(messageMap : messageList){
  13. 更新log訊息通知紀錄到CLN\_MESSAGE\_LOG(訊息發送記錄檔)

| |
| --- |
| - 1. update CLN\_MESSAGE\_LOG
  2. set NEWS\_STATUS = messageMap.NEWS\_STATUS,
  3. SEND\_COUNT = messageMap.SEND\_COUNT,
  4. OUTPUT\_TIME = 系統時間
  5. where APPLY\_ID = messageMap.APPLY\_ID
  6. and SEND\_TYPE = messageMap.SEND\_TYPE
  7. and INFM\_ID = messageMap.INFM\_ID
  8. and INPUT\_TIME = messageMap.INPUT\_TIME
  9. [and MEMO\_KEY = messageMap.MEMO\_KEY]
 |

- 1. }
  2. Transaction.commit();
  3. cntMap.put("outputcnt", cntMap.get("outputcnt") + messageList.count);//紀錄輸出件數
  4. messageList清空;
  5. }// If(messageList不為空 && (messageList >= 200筆 or 本次迴圈已經是extractstrList最後一筆了)) 結尾
  6. } // for(extractstrMap : extractstrList){ 結尾
  7. }// if (Math.abs((inputMM - writeStrMM) % CYCLE\_MIN) <= 1) { 結尾
  8. }// if (inputTime >= writeStrTime && inputTime <= writeEndTime) { 結尾
  9. }// if ((!isHoliday || "Y".equals(WRITE\_HOLIDAY))…… 結尾
  10. } //for (map: messageSettingList) {結尾
  11. }// If(messageSettingList 不為空){ 結尾
1. 紀錄執行結束時間

紀錄LOG：Log.info(@程式名稱 + “紀錄執行結束時間:” + 系統時間)

1. 例外處理

Log.fatal("執行時發生錯誤")

isSuccess = false;

1. FINALLY
  1. Transaction.begin();
  2. try {
  3. Log.info("輸入件數:" + cntMap.get("inputcnt") + "件 " + "輸出件數:" + cntMap.get("outputcnt") + "件")
  4. Log.info(@程式名稱 + “批次處理完成”)
  5. 更新CLN\_BATCH\_CTL(批次執行控制檔)
  6. if(isSuccess){
  7. String JOB\_STATUS = '9';
  8. }else{
  9. String JOB\_STATUS = '0';
  10. }

| |
| --- |
| - 1. update CLN\_BATCH\_CTL
  2. set JOB\_STATUS = @JOB\_STATUS,
  3. JOB\_END\_TIME = 系統時間
  4. where EXEC\_DATE = @startDate and JOB\_NAME = @程式名稱
 |

- 1. Transaction.commit();
  2. }catch (Exception e) {
  3. Transaction.rollback();
  4. Log.fatal("記錄批次執行結果發生錯誤")
  5. }
1. API規格
2. LIP-B-COMBUSSDQ001 查詢營業日
  1. API資訊
  2. API URI：/company/bussDay/query
  3. 服務名稱： 查詢結帳匯率API
  4. HTTP METHOD：POST
  5. CONTENT-TYPE：application/json
  6. 上行電文

| | | | | | |
| --- | --- | --- | --- | --- | --- |
| LVL | 欄位名稱 | 資料型態 | 最大長度 | 必要 | 欄位名稱及說明 |
| 1 | MWHEADER | | | | |
| 2 | MSGID | String | 20 | Y | |
| 2 | SOURCECHANNEL | String | 20 | Y | 來源端系統AP ID |
| 2 | TXNSEQ | String | 20 | Y | 交易序號，請於下行電文【TXNSEQ】欄位原值回傳 |
| 2 | RETURNCODE | String | 4 | Y | 上行無使用，固定帶空值 |
| 2 | RETURNDESC | String | 128 | Y | 上行無使用，固定帶空值 |
| 2 | O360SEQ | String | 20 | | 上行無使用，固定帶空TAG |
| 1 | TRANRQ | | | | |
| 2 | BaseDate | String | 10 | Y | 基準日 |

範例：

| |
| --- |
| {"MWHEADER": {"MSGID": "","SOURCECHANNEL": "","RETURNCODE": "","RETURNDESC": "","TXNSEQ": "","O360SEQ": ""},"TRANRQ": {"BaseDate": "",}} |

- 1. 下行電文

| | | | | | |
| --- | --- | --- | --- | --- | --- |
| LVL | 欄位名稱 | 資料型態 | 最大長度 | 必要 | 欄位名稱及說明 |
| 1 | MWHEADER | | | | |
| 2 | MSGID | String | 20 | Y | 同上行電文 |
| 2 | SOURCECHANNEL | String | 20 | Y | 同上行電文 |
| 2 | TXNSEQ | String | 20 | Y | 同上行電文 |
| 2 | RETURNCODE | String | 4 | Y | 處理結果代碼 |
| 2 | RETURNDESC | String | 128 | Y | 處理結果訊息 |
| 2 | O360SEQ | String | 20 | Y | 中台交易序號（由中台產製唯一交易序號） |
| 1 | TRANRS | | | | |
| 2 | BaseDate | String | 10 | Y | 基準日 |
| 2 | DtepBussFlag | String | 1 | Y | DTEP日曆參數檔中記錄本日是否營業的註記 |
| 2 | WorkingDayFlag | String | 1 | Y | 本日是否為WORKING DAY |
| 2 | MonthlyEndFlag | String | 1 | Y | 本日是否為當月份月底日(日曆日) |
| 2 | NextBussDate | String | 10 | Y | 次營業日 |
| 2 | PrevBussDate | String | 10 | Y | 前一營業日 |
| 2 | MonthlyEndDate | String | 10 | Y | 本月份月底日(日曆日) |
| 2 | MonthlyBegDate | String | 10 | Y | 本月份月初日(日曆日) |
| 2 | NextMonthBegDate | String | 10 | Y | 次月份月初日(日曆日) |

1. LIP-C-CMSSDSMST001簡訊發送服務
  1. API資訊
  2. API URI：/lon/msg/sendSMS
  3. 服務名稱： 簡訊發送 API
  4. HTTP METHOD：POST
  5. CONTENT-TYPE：application/json
  6. 上行電文

| | | | | | |
| --- | --- | --- | --- | --- | --- |
| LVL | 欄位名稱 | 資料型態 | 最大長度 | 必要 | 欄位名稱及說明 |
| 1 | MWHEADER | | | | |
| 2 | MSGID | String | 20 | Y | 電文代碼，半形英數字格式：LIP-C- CMSSDSMST001 |
| 2 | SOURCECHANNEL | String | 20 | Y | 來源端系統AP ID格式為LON-LX-LIP-01 |
| 2 | TXNSEQ | String | 20 | Y | 交易序號格式由來源端系統自訂，每筆交易需具唯一性 |
| 1 | TRANRQ | | | | |
| 2 | ModuleTypeCode | String | 10 | Y | 模組代碼 |
| 2 | SMSNotifyList | | | | 簡訊的集合，包含以下屬性 |
| 3 | CaseNbr | String | 20 | | 案件編號 |
| 3 | SMSNotifyContent | String | 50 | Y | 通知內容 |
| 3 | SMSNotifySubject | String | 50 | Y | 通知主旨 |
| 3 | NotifyCode | String | 10 | Y | 對外發信通知種類代碼 |
| 3 | RecipientCellPhone | String | 20 | Y | 收件人手機 |
| 3 | RecipientId | String | 11 | | 收件者統編 |
| 3 | RecipientName | String | 300 | Y | 收件人姓名 |
| 3 | AcntDeptCode | String | 5 | | 費用分攤單位費用單位代號 |
| 3 | SchSendTime | String | 20 | | 預定發送時間即時發送請給空值 |

範例：

| |
| --- |
| {"MWHEADER":{ "MSGID":"LIP-C- CMSSDSMST001", "SOURCECHANNEL":"LON-LX-LIP-01", "TXNSEQ":" 2021051816583755" }, "TRANRQ":{ "ModuleTypeCode": "PLN", "mailNotifyList": [ { "CaseNbr": "", " SMSNotifyContent ": "客戶您好…", " SMSNotifySubject ": "對保通知” "NotifyCode": "L97", " RecipientCellPhone ": "0911011111", "RecipientId": "A123456789", "RecipientName": "陳XX", “AcntDeptCode”:”000807”, “SchSendTime”:”2021/08/19 15:00” },} |

- 1. 下行電文

| | | | | | |
| --- | --- | --- | --- | --- | --- |
| LVL | 欄位名稱 | 資料型態 | 最大長度 | 必要 | 欄位名稱及說明 |
| 1 | MWHEADER | | | | |
| 2 | MSGID | String | 20 | Y | 同上行電文 |
| 2 | SOURCECHANNEL | String | 20 | Y | 同上行電文 |
| 2 | TXNSEQ | String | 20 | Y | 同上行電文 |
| 2 | RETURNCODE | String | 4 | Y | 處理結果代碼若UUID集合中全數成功為0000 ，判斷本次寫入簡訊含任一筆發送失敗則回傳E0230000 - 表示處理成功9999 - 表示處理有誤E023 – 此批發送簡訊部分失敗 |
| 2 | RETURNDESC | String | 128 | | 處理結果訊息 |
| 1 | TRANRS | | | | |
| 2 | ModuleTypeCode | String | 10 | Y | 模組代碼 |
| 2 | SMSlNotifyItemTranrs | String | | Y | UUID 集合 |
| 3 | SMSlNotifyNbr | String | 50 | Y | UUID |
| 3 | CaseNbr | String | 20 | | 案件編號 |
| 3 | NotifyCode | String | 10 | Y | 對外發信通知種類代碼 |
| 3 | RecipientCellPhone | String | 20 | Y | 收件人手機 |
| 3 | RecipientId | String | 11 | | 收件者統編 |
| 3 | RecipientName | String | 300 | Y | 收件人姓名 |
| 3 | ReturnDesc | String | 2000 | | 逐筆UUID處理結果訊息 |
| 2 | ModuleTypeCode | String | 10 | Y | 模組代碼 |
| 2 | SMSlNotifyItemTranrs | String | | Y | UUID 集合 |
| 3 | SMSlNotifyNbr | String | 50 | Y | UUID |
| 2 | ModuleTypeCode | String | 10 | Y | 模組代碼 |
| 2 | SMSlNotifyItemTranrs | String | | Y | UUID 集合 |
| 3 | SMSlNotifyNbr | String | 50 | Y | UUID |

1. LIP-C-CMSSMAILT001 行內信寄送
  1. API資訊
  2. API URI：
  3. 服務名稱： 行內信寄送
  4. HTTP METHOD：POST
  5. CONTENT-TYPE：application/json
  6. 上行電文

| | | | | | |
| --- | --- | --- | --- | --- | --- |
| LVL | 欄位名稱 | 資料型態 | 最大長度 | 必要 | 欄位名稱及說明 |
| 1 | MWHEADER | | | | |
| 2 | MSGID | String | 20 | Y | 電文代碼，半形英數字格式：LIP-C-CMSSMAILT001 |
| 2 | SOURCECHANNEL | String | 20 | Y | 來源端系統AP ID格式為LON-LX-LIP-01 |
| 2 | TXNSEQ | String | 20 | Y | 交易序號格式由來源端系統自訂，每筆交易需具唯一性 |
| 1 | TRANRQ | | | | |
| 2 | ModuleTypeCode | String | 10 | Y | 模組代碼 |
| 2 | SenderEmail | String | 100 | | 寄件人E-MAIL若為空則以模組代碼取得預設寄件人 |
| 2 | SenderName | String | 20 | | 寄件人名稱 |
| 2 | MailNotifyList | | | | 信件的集合，包含以下屬性 |
| 3 | CaseNbr | String | 20 | | 案件編號 |
| 3 | MailNotifyCode | String | 1 | Y | 1:行內2:行外 |
| 3 | MailNotifyContent | String | 2000 | Y | 信件通知內容 |
| 3 | MailNotifySubject | String | 150 | Y | 信件通知主旨 |
| 3 | MailTypeCode | String | 1 | | 發信類型1:純文字 2:MIME(html標籤) 3:模版發信 4:BillHunter |
| 3 | NotifyCode | String | 10 | | 對外發信通知種類代碼 |
| 3 | RecipientList | | | | |
| 4 | RecipientEmail | String | 500 | Y | 收件人E-MAIL |
| 4 | RecipientName | String | 30 | | 收件人姓名 |
| 4 | CcFlag | String | 1 | | Y:副本/N:正本/B:密件副本 |
| 3 | Attachment | String | | | 附件檔名，檔名以，間隔(只允許MailTypeCode: 2 or 3) |
| 3 | ContentTemplateCode | String | | | 內容版型代號 |
| 3 | ContentParaValue | String | | | 模板發信參數內容(只允許 MailTypeCode: 3) |

範例：

| |
| --- |
| { "MWHEADER":{ "MSGID":"LIP-C-CMSSMAILT001", "SOURCECHANNEL":"LON-LX-LIP-01", "TXNSEQ":" 2021051816583755" }, "TRANRQ":{ "ModuleTypeCode": "LON", "MailNotifyList": [ { "Attachment": "", "CaseNbr": "D110068220124", "ContentParaValue": "", "ContentTemplateCode": "", "MailNotifyCode": 1, "MailNotifyContent": "iLending 授信整合平台 (純文字)", "MailNotifySubject": "通知主旨1", "MailTypeCode": 1, "NotifyCode": "", "RecipientList": [ { "RecipientEmail": "NT87051@cathaybk.com.tw", "RecipientName": "陳XX", "CcFlag": "N" } ] }, { "Attachment": "", "CaseNbr": "D110068220124", "ContentParaValue": "", "ContentTemplateCode": "", "MailNotifyCode": 1, "MailNotifyContent": "<h1>iLending 授信整合平台</h1><br><p>這是p標籤內容</p>", "MailNotifySubject": "通知主旨2", "MailTypeCode": 2, "NotifyCode": "", "RecipientList": [ { "RecipientEmail": "NT87051@cathaybk.com.tw", "RecipientName": "陳XX", "CcFlag": "N" } ] }, { "Attachment": "", "CaseNbr": "D110068220124", "ContentParaValue": "", "ContentTemplateCode": "", "MailNotifyCode": 1, "MailNotifyContent": "iLending 授信整合平台", "MailNotifySubject": "通知主旨3", "MailTypeCode": 3, "NotifyCode": "", "RecipientList": [ { "RecipientEmail": "NT87051@cathaybk.com.tw", "RecipientName": "陳XX", "CcFlag": "N" } ] } ] }} |

- 1. 下行電文

| | | | | | |
| --- | --- | --- | --- | --- | --- |
| LVL | 欄位名稱 | 資料型態 | 最大長度 | 必要 | 欄位名稱及說明 |
| 1 | MWHEADER | | | | |
| 2 | MSGID | String | 20 | Y | 同上行電文 |
| 2 | SOURCECHANNEL | String | 20 | Y | 同上行電文 |
| 2 | TXNSEQ | String | 20 | Y | 同上行電文 |
| 2 | RETURNCODE | String | 20 | Y | 處理結果代碼0000 - 表示處理成功其他 - 表示處理有誤 |
| 2 | RETURNDESC | String | 20 | Y | 處理結果訊息 |
| 1 | TRANRS | | | | |
| 2 | Result | String | | Y | 寄送結果 |


