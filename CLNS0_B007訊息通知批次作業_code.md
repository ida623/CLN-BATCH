package cub.lip.bch.cln.tasklet;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import cub.lip.bch.cln.dto.cmss.*;
import cub.lip.bch.cln.entity.ClnMessageLogEntity;
import cub.lip.bch.cln.repository.ClnMessageLogRepository;
import cub.lip.lib.cln.common.base.dto.ChkMessageVo;
import cub.lip.lib.cln.common.base.dto.ClnMessageLogVo;
import cub.lip.lib.cln.common.util.MessageUtil;
import cub.lip.lib.common.dto.base.ResponseTemplate;
import cub.lip.lib.common.enums.WorkingDayEnum;
import cub.lip.lib.common.util.resttemplate.RestTemplateHelper;
import cub.lip.lib.common.util.sql.SqlAction;
import cub.lip.lib.common.util.sql.SqlUtils;
import cub.lip.lib.common.util.transfer.NormalRequestFactory;
import cub.lip.lib.common.util.valid.ValidWorkingdayUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * CLNS0_B007 訊息通知批次作業 Tasklet
 * 
 * @author 00550381
 */
@Slf4j
@Component
@StepScope
public class CLNS0B007MessageSendTasklet implements Tasklet {

  /** SqlUtils */
  @Autowired
  private SqlUtils sqlUtils;

  /** SqlAction */
  @Autowired
  private SqlAction sqlAction;

  /** ClnMessageLogRepository */
  @Autowired
  private ClnMessageLogRepository clnMessageLogRepository;

  /** NormalRequestFactory */
  @Autowired
  private NormalRequestFactory normalReqFactory;

  /** RestTemplateHelper */
  @Autowired
  private RestTemplateHelper restTemplateHelper;

  /** MessageUtil */
  @Autowired
  private MessageUtil messageUtil;

  /** ValidWorkingdayUtil */
  @Autowired
  private ValidWorkingdayUtil validWorkingdayUtil;

  /** args */
  @Value("#{jobParameters[args]}")
  private String args;

  /** txnSeq */
  @Value("#{jobParameters[txnSeq]}")
  private String txnSeq;

  /** SourceChannel */
  @Value("${cub.ap-id}")
  private String sourceChannel;

  /** CLNS0B007_QueryMessageSetting */
  private static final String SQL_Q001 = "CLNS0B007_QueryMessageSetting_Q001.sql";

  /** CLNS0B007_InsertMessageHistory */
  private static final String SQL_I001 = "CLNS0B007_InsertMessageHistory_I001.sql";

  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

    Map<String, Integer> cntMap = new HashMap<>();
    cntMap.put("inputcnt", 0);
    cntMap.put("outputcnt", 0);

    // 2. 設定參數
    String[] inputArgs = StringUtils.isEmpty(args) ? new String[0] : args.split(",", -1);
    String inputDate;
    String inputTime;
    String sendType;
    LocalDateTime now = LocalDateTime.now();
    String defaultDate = now.toLocalDate().format(DateTimeFormatter.BASIC_ISO_DATE);
    String defaultTime = now.toLocalTime().format(DateTimeFormatter.ofPattern("HHmm"));

    if (inputArgs.length == 0) {
      inputDate = defaultDate;
      inputTime = defaultTime;
      sendType = StringUtils.EMPTY;
    } else {
      inputDate = inputArgs[0];
      try {
        LocalDate.parse(inputDate, DateTimeFormatter.BASIC_ISO_DATE);
      } catch (Exception e) {
        log.error("傳入參數非日期格式 : {}", inputDate);
        logBatchCount(cntMap);
        return RepeatStatus.FINISHED;
      }

      if (inputArgs.length == 1) {
        inputTime = defaultTime;
        sendType = StringUtils.EMPTY;
      } else if (inputArgs.length == 2) {
        inputTime = inputArgs[1];
        sendType = StringUtils.EMPTY;
      } else if (inputArgs.length == 3) {
        inputTime = inputArgs[1];
        sendType = inputArgs[2];
      } else {
        log.error("傳入參數個數有誤 : 傳入長度={}", inputArgs.length);
        logBatchCount(cntMap);
        return RepeatStatus.FINISHED;
      }

      try {
        LocalTime.parse(inputTime, DateTimeFormatter.ofPattern("HHmm"));
      } catch (Exception e) {
        log.error("傳入參數非時間格式 : {}", inputTime);
        logBatchCount(cntMap);
        return RepeatStatus.FINISHED;
      }

    }

    // 3. CLN_MESSAGE_LOG(訊息發送記錄檔)只保留近6個月的檔案，超過則搬到CLN_MESSAGE_HISTORY(訊息發送歷史檔)
    LocalDate inputDateMinus6Months = LocalDate.parse(inputDate, DateTimeFormatter.BASIC_ISO_DATE).minusMonths(6L);
    String inputDateS6M = inputDateMinus6Months.format(DateTimeFormatter.BASIC_ISO_DATE);

    Map<String, Object> historyParamMap = new HashMap<>();
    historyParamMap.put("InputDateS6M", inputDateS6M);
    sqlAction.update(sqlUtils.getQuerySql(SQL_I001), historyParamMap);

    LocalDateTime cutoffDateTime = inputDateMinus6Months.atStartOfDay();
    clnMessageLogRepository.deleteByInputTimeLessThanEqualAndNewsStatus(Timestamp.valueOf(cutoffDateTime), 3);

    // 4. 處理本次訊息通知
    // 4.1 查詢CLN_MESSAGE_SETTING (訊息來源控制檔)
    Map<String, Object> messageSettingParamMap = new HashMap<>();
    messageSettingParamMap.put("InputDate", inputDate);
    messageSettingParamMap.put("SendType", StringUtils.trimToNull(sendType));

    List<Map<String, Object>> messageSettingList = sqlAction.queryForList(sqlUtils.getQuerySql(SQL_Q001), messageSettingParamMap);

    if (CollectionUtils.isNotEmpty(messageSettingList)) {

      List<ClnMessageLogVo> messageList = new ArrayList<>();

      // 當作 messageList 的 index，後續更新狀態用
      Map<String, ClnMessageLogVo> messageLogBySendKey = new HashMap<>();

      // 收集簡訊清單，一次發送
      List<CMSSDSMST001TranrqSmsNotify> smsList = new ArrayList<>();
      // 收集 email 清單，一次發送
      List<CMSSMAILT001TranrqMailNotify> emailList = new ArrayList<>();

      // 4.2 確認是否為假日
      Optional<Boolean> isHolidayOptional = isInputDateHoliday(inputDate);
      if (!isHolidayOptional.isPresent()) {
        logBatchCount(cntMap);
        return RepeatStatus.FINISHED;
      }
      boolean isHoliday = isHolidayOptional.get();

      for (Map<String, Object> messageSetting : messageSettingList) {
        if (!processMessageSendSetting(messageSetting, inputDate, inputTime, isHoliday, cntMap, messageList, messageLogBySendKey,
          smsList, emailList)) {
          logBatchCount(cntMap);
          return RepeatStatus.FINISHED;
        }
      }

      flushMessageBatch(messageList, messageLogBySendKey, cntMap, smsList, emailList);

    }

    logBatchCount(cntMap);
    return RepeatStatus.FINISHED;
  }

  /**
   * 確認是否為假日
   */
  private Optional<Boolean> isInputDateHoliday(String inputDate) {
    try {
      LocalDateTime inputDateTime = LocalDate.parse(inputDate, DateTimeFormatter.BASIC_ISO_DATE).atStartOfDay();
      boolean isWorkingDay = validWorkingdayUtil.validWorkingday(WorkingDayEnum.WORKING_DAY.getBussDateCheckMode(), inputDateTime);
      return Optional.of(!isWorkingDay);
    } catch (Exception e) {
      log.error("檢核營業日失敗, inputDate={}", inputDate, e);
      return Optional.empty();
    }
  }

  /**
   * 是否成功建立 SMS/Email 待發送清單
   */
  private boolean processMessageSendSetting(Map<String, Object> messageSetting, String inputDate, String inputTime, boolean isHoliday,
      Map<String, Integer> cntMap, List<ClnMessageLogVo> messageList, Map<String, ClnMessageLogVo> messageLogBySendKey,
      List<CMSSDSMST001TranrqSmsNotify> smsList, List<CMSSMAILT001TranrqMailNotify> emailList) throws Exception {

    String writeStrTime = MapUtils.getString(messageSetting, "WRITE_STR_TIME");
    String writeEndTime = MapUtils.getString(messageSetting, "WRITE_END_TIME");

    if (StringUtils.isAnyBlank(writeStrTime, writeEndTime)) {
      return true;
    }

    String writeHoliday = MapUtils.getString(messageSetting, "WRITE_HOLIDAY");
    int cycleMin = MapUtils.getIntValue(messageSetting, "CYCLE_MIN", 0);
    if (cycleMin <= 0) {
      return true;
    }

    int currentInputTime = Integer.parseInt(inputTime);
    int writeStart = Integer.parseInt(writeStrTime);
    int writeEnd = Integer.parseInt(writeEndTime);
    Timestamp inputTimestamp = Timestamp.valueOf(
      LocalDate.parse(inputDate, DateTimeFormatter.BASIC_ISO_DATE).atTime(currentInputTime / 100, currentInputTime % 100));

    // 平日一律執行，假日則需看 table 設定，再決定是否要執行
    if ((isHoliday && !"Y".equals(writeHoliday)) || currentInputTime < writeStart || currentInputTime > writeEnd) {
      return true;
    }

    int inputMinute = currentInputTime / 100 * 60 + currentInputTime % 100;
    int writeStartMinute = writeStart / 100 * 60 + writeStart % 100;

    // 誤差超過 1 分鐘
    if (Math.abs((inputMinute - writeStartMinute) % cycleMin) > 1) {
      return true;
    }

    // 取得簡訊內容參數，取得每一個寄信對象資料 extractstrList
    String extractStr = MapUtils.getString(messageSetting, "EXTRACTSTR");
    List<Map<String, Object>> extractstrList = Collections.emptyList();
    if (StringUtils.isNotBlank(extractStr)) {
      Map<String, Object> paramMap = new HashMap<>();
      paramMap.put("InputDate", inputDate);
      String parsedSql = messageUtil.replaceParam(extractStr, paramMap);
      extractstrList = sqlAction.queryForList(parsedSql, Collections.emptyMap());
    }

    cntMap.put("inputcnt", cntMap.get("inputcnt") + extractstrList.size());

    String sendType = StringUtils.defaultString(MapUtils.getString(messageSetting, "SEND_TYPE"));
    String subject = StringUtils.defaultString(MapUtils.getString(messageSetting, "TITLE"));
    String msgTemplate = StringUtils.defaultString(MapUtils.getString(messageSetting, "MSG"));

    for (Map<String, Object> extractstrMap : extractstrList) {
      String message = messageUtil.replaceParam(msgTemplate, extractstrMap);

      ClnMessageLogVo messageLog = buildMessageLogVo(extractstrMap, sendType, subject, message, inputTimestamp);

      // 資料檢核
      ChkMessageVo chkMessage = messageUtil.chkMessageParams(messageLog);
      String isContinue = chkMessage.getIsContinue();
      String errorMessage = chkMessage.getErrorMessage();
      if ("Y".equals(isContinue) && StringUtils.isNotBlank(errorMessage)) {
        log.info(errorMessage);
        continue;
      }

      String notifyKind = StringUtils.left(messageLog.getSendType(), 1);
      // A: SMS 簡訊
      if ("A".equals(notifyKind)) {
        CMSSDSMST001TranrqSmsNotify smsItem = new CMSSDSMST001TranrqSmsNotify();
        smsItem.setCaseNbr(messageLog.getApplyId());
        smsItem.setSmsNotifyContent(messageLog.getNews());
        smsItem.setSmsNotifySubject(messageLog.getSubject());
        smsItem.setNotifyCode(messageLog.getNotifyType());
        smsItem.setRecipientCellPhone(messageLog.getMobile());
        smsItem.setRecipientId(messageLog.getInfmId());
        smsItem.setRecipientName(messageLog.getInfmName());
        smsItem.setAcntDeptCode(MapUtils.getString(extractstrMap, "REMIT_BRANCH"));
        smsList.add(smsItem);
        messageLogBySendKey.put(buildSmsKey(smsItem.getCaseNbr(), smsItem.getNotifyCode(), smsItem.getRecipientCellPhone()),
          messageLog);

        // B:INTERNAL-EMAIL 內部郵件// C: EXTERNAL-EMAIL 外部郵件
      } else if ("B".equals(notifyKind) || "C".equals(notifyKind)) {
        CMSSMAILT001TranrqRecipient recipient = new CMSSMAILT001TranrqRecipient();
        recipient.setRecipientEmail(messageLog.getEmail());
        recipient.setRecipientName(messageLog.getInfmName());
        recipient.setCcFlag("N");

        List<CMSSMAILT001TranrqRecipient> recipientList = new ArrayList<>();
        recipientList.add(recipient);

        CMSSMAILT001TranrqMailNotify emailItem = new CMSSMAILT001TranrqMailNotify();
        emailItem.setCaseNbr(messageLog.getApplyId());
        emailItem.setMailNotifyCode("B".equals(notifyKind) ? "1" : "2");
        emailItem.setMailNotifyContent(messageLog.getNews());
        emailItem.setMailNotifySubject(messageLog.getSubject());
        emailItem.setMailTypeCode("2");
        emailItem.setNotifyCode(messageLog.getNotifyType());
        emailItem.setRecipientList(recipientList);
        emailList.add(emailItem);
        String recipientEmail = CollectionUtils.isEmpty(emailItem.getRecipientList()) ? ""
            : emailItem.getRecipientList().get(0).getRecipientEmail();
        messageLogBySendKey.put(buildEmailKey(emailItem.getCaseNbr(), emailItem.getMailNotifySubject(), recipientEmail),
          messageLog);
      } else {
        log.error("未支援類型 [{}]", notifyKind);
        return false;
      }

      // 新增進通知清單，計算兩百筆使用
      messageList.add(messageLog);

      // 如果超過 200 筆，則先送出一次
      if (messageList.size() >= 200) {
        flushMessageBatch(messageList, messageLogBySendKey, cntMap, smsList, emailList);
      }
    }

    return true;
  }

  /**
   * 送出 SMS/Email 待發送清單，更新訊息紀錄與批次筆數
   */
  private void flushMessageBatch(List<ClnMessageLogVo> messageList, Map<String, ClnMessageLogVo> messageLogBySendKey,
      Map<String, Integer> cntMap, List<CMSSDSMST001TranrqSmsNotify> smsList, List<CMSSMAILT001TranrqMailNotify> emailList) {

    // 呼叫【LIP-C-CMSSDSMST001簡訊發送服務】
    if (CollectionUtils.isNotEmpty(smsList)) {
      sendSms(smsList, messageLogBySendKey);
      smsList.clear();
    }

    // 呼叫【LIP-C-CMSSMAILT001 行內信寄送】
    if (CollectionUtils.isNotEmpty(emailList)) {
      sendEmail(emailList, messageLogBySendKey);
      emailList.clear();
    }

    // 新增 log 訊息通知紀錄
    if (CollectionUtils.isNotEmpty(messageList)) {
      saveMessageLogs(messageList);
      cntMap.put("outputcnt", cntMap.get("outputcnt") + messageList.size());
      messageList.clear();
      messageLogBySendKey.clear();
    }
  }

  /**
   * 呼叫【LIP-C-CMSSDSMST001 簡訊發送服務】
   */
  private void sendSms(List<CMSSDSMST001TranrqSmsNotify> smsList, Map<String, ClnMessageLogVo> messageLogBySendKey) {
    CMSSDSMST001Tranrq tranrq = new CMSSDSMST001Tranrq();
    tranrq.setModuleTypeCode("CLN");
    tranrq.setSmsNotifyList(smsList);

    ResponseTemplate<CMSSDSMST001Tranrs> response = restTemplateHelper.postForEntity(
      normalReqFactory.genRequestTemplate(tranrq, "LIP-C-CMSSDSMST001", sourceChannel, txnSeq), CMSSDSMST001Tranrs.class);

    String returnCode = response.getMwHeaderResponse().getReturnCode();
    if (!"0000".equals(returnCode) && response.getTranrs() != null
        && CollectionUtils.isNotEmpty(response.getTranrs().getSmsNotifyItemTranrs())) {
      for (CMSSDSMST001TranrsSmsNotify smsResult : response.getTranrs().getSmsNotifyItemTranrs()) {
        // 不成功的訊息，才會更新為失敗
        if (!"Success".equalsIgnoreCase(smsResult.getReturnDesc())) {
          ClnMessageLogVo messageLog = messageLogBySendKey
              .get(buildSmsKey(smsResult.getCaseNbr(), smsResult.getNotifyCode(), smsResult.getRecipientCellPhone()));

          // 更新狀態為失敗
          if (messageLog != null) {
            messageLog.setNewsStatus(-1);
          }
        }
      }
    }
  }

  /**
   * 呼叫【LIP-C-CMSSMAILT001 行內信寄送】
   */
  private void sendEmail(List<CMSSMAILT001TranrqMailNotify> emailList, Map<String, ClnMessageLogVo> messageLogBySendKey) {
    CMSSMAILT001Tranrq tranrq = new CMSSMAILT001Tranrq();
    tranrq.setModuleTypeCode("CLN");
    tranrq.setMailNotifyList(emailList);

    ResponseTemplate<CMSSMAILT001Tranrs> response = restTemplateHelper.postForEntity(
      normalReqFactory.genRequestTemplate(tranrq, "LIP-C-CMSSMAILT001", sourceChannel, txnSeq), CMSSMAILT001Tranrs.class);

    // 把這次的 emailList 都更新為失敗
    if (!"0000".equals(response.getMwHeaderResponse().getReturnCode())) {
      for (CMSSMAILT001TranrqMailNotify emailItem : emailList) {
        String recipientEmail = CollectionUtils.isEmpty(emailItem.getRecipientList()) ? ""
            : emailItem.getRecipientList().get(0).getRecipientEmail();
        ClnMessageLogVo messageLog = messageLogBySendKey
            .get(buildEmailKey(emailItem.getCaseNbr(), emailItem.getMailNotifySubject(), recipientEmail));

        // 更新狀態為失敗
        if (messageLog != null) {
          messageLog.setNewsStatus(-1);
        }
      }
    }
  }

  /**
   * 將查詢結果轉成 MessageLogVo
   */
  private ClnMessageLogVo buildMessageLogVo(Map<String, Object> sourceMap, String sendType, String subject, String message,
      Timestamp inputTimestamp) {
    ClnMessageLogVo messageLog = new ClnMessageLogVo();
    messageLog.setApplyId(MapUtils.getString(sourceMap, "APPLY_ID"));
    messageLog.setSendType(sendType);
    messageLog.setNewsStatus(3);
    messageLog.setCnt(MapUtils.getInteger(sourceMap, "CNT"));
    messageLog.setInfmId(StringUtils.defaultString(MapUtils.getString(sourceMap, "INFM_ID")));
    messageLog.setInfmName(StringUtils.defaultString(MapUtils.getString(sourceMap, "INFM_NAME")));
    messageLog.setSubject(subject);
    messageLog.setEmail(MapUtils.getString(sourceMap, "EMAIL"));
    messageLog.setMobile(MapUtils.getString(sourceMap, "MOBILE"));
    messageLog.setNews(message);
    messageLog.setInputTime(inputTimestamp);
    messageLog.setOutputTime(Timestamp.valueOf(LocalDateTime.now()));
    messageLog.setSendCount(1);
    messageLog.setNotifyType(StringUtils.defaultString(MapUtils.getString(sourceMap, "NOTIFY_TYPE")));
    messageLog.setMemoKey(StringUtils.defaultString(MapUtils.getString(sourceMap, "MEMO_KEY")));
    return messageLog;
  }

  /**
   * 儲存 MessageLog
   */
  private void saveMessageLogs(List<ClnMessageLogVo> messageList) {
    List<ClnMessageLogEntity> entities = messageList.stream().map(vo -> {
      ClnMessageLogEntity entity = new ClnMessageLogEntity();

      // ClnMessageLogVo 比 ClnMessageLogEntity 多了 Cnt 欄位，使用手動轉換
      entity.setApplyId(vo.getApplyId());
      entity.setSendType(vo.getSendType());
      entity.setNewsStatus(vo.getNewsStatus());
      entity.setInfmId(vo.getInfmId());
      entity.setInfmName(vo.getInfmName());
      entity.setSubject(vo.getSubject());
      entity.setEmail(vo.getEmail());
      entity.setMobile(vo.getMobile());
      entity.setNews(vo.getNews());
      entity.setInputTime(vo.getInputTime());
      entity.setOutputTime(vo.getOutputTime());
      entity.setSendCount(vo.getSendCount());
      entity.setNotifyType(vo.getNotifyType());
      entity.setMemoKey(vo.getMemoKey());
      return entity;
    }).collect(Collectors.toList());

    clnMessageLogRepository.saveAll(entities);
  }

  /**
   * 產生 SMS key
   */
  private String buildSmsKey(String applyId, String notifyType, String mobile) {
    return String.join("|", StringUtils.defaultString(applyId), StringUtils.defaultString(notifyType),
      StringUtils.defaultString(mobile));
  }

  /**
   * 組裝 Email key
   */
  private String buildEmailKey(String applyId, String subject, String email) {
    return String.join("|", StringUtils.defaultString(applyId), StringUtils.defaultString(subject), StringUtils.defaultString(email));
  }

  /**
   * 紀錄批次處理件數
   */
  private void logBatchCount(Map<String, Integer> cntMap) {
    log.info("輸入件數:{}件 輸出件數:{}件", cntMap.get("inputcnt"), cntMap.get("outputcnt"));
  }

}

