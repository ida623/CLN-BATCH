package cub.lip.bch.elc.job;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import cub.lip.bch.elc.listener.ELCV0B002ExportStepListener;
import cub.lip.bch.elc.processor.ELCV0B002ExportBodyProcessor;
import cub.lip.bch.elc.tasklet.ELCV0B002BackupTasklet;
import cub.lip.bch.elc.tasklet.ELCV0B002ImportDbTasklet;
import cub.lip.bch.elc.util.BytePadUtil;
import cub.lip.bch.elc.vo.ELCV0B002ExportBodyVo;
import cub.lip.bch.elc.vo.ELCV0B002ExportHeaderVo;
import cub.lip.bch.elc.vo.ELCV0B002ExportParamsVo;
import cub.lip.lib.common.util.sql.SqlUtils;
import cub.lip.svc.batch.flow.PreBatchCheckFlowConfig;
import cub.lip.svc.batch.listener.SimpleJobListener;

/**
 * ELCV0_B002 表外帳產F002系統分錄檔批次作業
 * 
 * @author 00550352
 */
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@Configuration
public class ELCV0B002JobConfig {

    /** 每批數量 */
    @Value("${lip.batch.fetch-size}")
    private int fetchSize;

    /** JobBuilderFactory */
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    /** StepBuilderFactory */
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    /** PreBatchCheckFlowConfig */
    @Autowired
    private PreBatchCheckFlowConfig preBatchCheck;

    /** DataSource */
    @Autowired
    private DataSource dataSource;

    /** SqlUtils */
    @Autowired
    private SqlUtils sqlUtils;

    /** ELCV0B002_BODY_QUERY001 */
    private static final String ELCV0B002_BODY_QUERY001 = "ELCV0B002_Body_Q001.sql";

    @Bean
    public Job ELCV0B002Job(
            SimpleJobListener listener, 
            @Qualifier("ELCV0B002ImportDbStep") Step importStep, 
            @Qualifier("ELCV0B002ExportStep") Step exportStep,
            @Qualifier("ELCV0B002BackupStep") Step backupStep) {
        return jobBuilderFactory.get("ELCV0_B002")
                .start(preBatchCheck.preBatchCheckFlow())
                .next(importStep)
                .next(exportStep)
                .next(backupStep)
                .end()
                .listener(listener)
                .build();
    }

    /**
     * 寫入ELC_ASSET_OFF_BL_ACCT_F002 表外帳F002系統分錄暫存檔
     * @param transactionManager
     * @param tasklet
     * @return
     */
    @Bean
    public Step ELCV0B002ImportDbStep(PlatformTransactionManager transactionManager, ELCV0B002ImportDbTasklet tasklet) {
        return stepBuilderFactory.get("ELCV0B002ImportDbStep")
                .tasklet(tasklet)
                .transactionManager(transactionManager)
                .build();
    }

    /**
     * 輸出檔案
     * @param ELCV0B002ExportReader
     * @param ELCV0B002BodyProcessor
     * @param ELCV0B002Writer
     * @param listener
     * @return
     */
    @Bean
    public Step ELCV0B002ExportStep(JdbcCursorItemReader<ELCV0B002ExportBodyVo> ELCV0B002ExportReader,
            ELCV0B002ExportBodyProcessor ELCV0B002BodyProcessor, FlatFileItemWriter<ELCV0B002ExportBodyVo> ELCV0B002Writer,
            ELCV0B002ExportStepListener listener) {
        return stepBuilderFactory.get("ELCV0B002ExportStep")
                .<ELCV0B002ExportBodyVo, ELCV0B002ExportBodyVo> chunk(fetchSize)
                .reader(ELCV0B002ExportReader)
                .processor(ELCV0B002BodyProcessor)
                .writer(ELCV0B002Writer)
                .listener(listener)
                .build();
    }

    /**
     * 備份/刪除檔案
     * @param transactionManager
     * @param tasklet
     * @return
     */
    @Bean
    public Step ELCV0B002BackupStep(PlatformTransactionManager transactionManager, ELCV0B002BackupTasklet tasklet) {
        return stepBuilderFactory.get("ELCV0B002BackupStep")
                .tasklet(tasklet)
                .transactionManager(transactionManager)
                .build();
    }
    
    /**
     * Reader 讀取檔案
     * @param dataDate
     * @return
     * @throws IOException
     */
    @Bean
    @StepScope
    public JdbcCursorItemReader<ELCV0B002ExportBodyVo> ELCV0B002ExportReader(@Value("#{jobParameters[dataDate]}")
    String dataDate) throws IOException {
        JdbcCursorItemReader<ELCV0B002ExportBodyVo> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql(sqlUtils.getQuerySql(ELCV0B002_BODY_QUERY001));
        reader.setPreparedStatementSetter(
            ps -> ps.setDate(1, Date.valueOf(LocalDate.parse(dataDate, DateTimeFormatter.ofPattern("yyyyMMdd")))));
        reader.setRowMapper(new BeanPropertyRowMapper<>(ELCV0B002ExportBodyVo.class));
        return reader;
    }

    /**
     * Writer 輸出檔案
     * @param params
     * @param headerVo
     * @return
     */
    @Bean
    @StepScope
    public FlatFileItemWriter<ELCV0B002ExportBodyVo> ELCV0B002Writer(
            @Value("#{stepExecutionContext['ELCV0B002_PARAMS']}") ELCV0B002ExportParamsVo params, 
            @Value("#{stepExecutionContext['ELCV0B002_HEADER']}") ELCV0B002ExportHeaderVo headerVo) {

        String fileEncode = StringUtils.defaultIfBlank(params.getFileEncode(), "MS950");
        Path tempPath = Paths.get(params.getTempExpPath(), params.getIoFileName());
        String[] names = { "dataType", "systemId", "journalCategory", "proposeDeptCode", "acctDateStr", "collCcyCode", "debitCreditType",
                "collAmtStr", "acctCode1BookingUnit", "acctCode2Group", "acctCode3Subsidiary", "acctCode4ProductCode",
                "acctCode5ProjectCode", "acctCode6Classification", "entryName", "entryDesc", "printedFlag", "reserved1", "reserved2",
                "companyCode", "bookCategory", "txDateStr", "acctCode7Filler", "fxRate", "functionalAmt", "extraDesc" };

        String eof = StringUtils.defaultIfBlank(params.getIoFileEOF(), "");

        return new FlatFileItemWriterBuilder<ELCV0B002ExportBodyVo>().name("ELCV0B002Writer")
                .resource(new FileSystemResource(tempPath))
                .transactional(true)
                .shouldDeleteIfExists(true)
                .encoding(fileEncode)
                .delimited()
                .delimiter("")
                .names(names)
                .headerCallback(writer -> {
                    String headerLine = buildHeader50(headerVo, Charset.forName(fileEncode));
                    writer.write(headerLine);
                })
                .footerCallback(writer -> {
                    if (StringUtils.isNotBlank(eof)) {
                        writer.write(eof);
                    }
                })
                .build();
    }


    /**
     * 設定表頭
     * @param h
     * @param cs
     * @return
     */
    private String buildHeader50(ELCV0B002ExportHeaderVo h, Charset cs) {
        String f1 = BytePadUtil.truncateAndPadString(h.getDataType(), 3, " ", false, cs);
        String f2 = BytePadUtil.truncateAndPadString(h.getSystemId(), 10, " ", false, cs);
        String f3 = BytePadUtil.truncateAndPadString(h.getDataDate(), 14, " ", false, cs);
        String f4 = BytePadUtil.truncateAndPadString(h.getDataCnt(), 10, "0", true, cs);
        String f5 = BytePadUtil.truncateAndPadString(h.getAcctDate(), 8, " ", false, cs);
        String f6 = BytePadUtil.truncateAndPadString(h.getCompanyCode(), 4, " ", false, cs);
        String f7 = BytePadUtil.truncateAndPadString(h.getBookCategory(), 1, " ", false, cs);
        String line = f1 + f2 + f3 + f4 + f5 + f6 + f7;
        if (line.getBytes(cs).length != 50)
            throw new IllegalStateException("Header 長度不為 50 bytes，實際=" + line.getBytes(cs).length);
        return line;
    }

}

package cub.lip.bch.elc.listener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import cub.lip.bch.elc.vo.ELCV0B002ExportHeaderVo;
import cub.lip.bch.elc.vo.ELCV0B002ExportParamsVo;
import cub.lip.lib.common.util.sql.SqlAction;
import cub.lip.lib.common.util.sql.SqlUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ELCV0B002ExportStepListener implements StepExecutionListener {

    /** SqlAction */
    @Autowired
    private SqlAction sqlAction;

    /** SqlUtils */
    @Autowired
    private SqlUtils sqlUtils;

    /** ELCV0B002_HEADER_QUERY001 */
    private static final String ELCV0B002_HEADER_QUERY001 = "ELCV0B002_Header_Q001.sql";

    /** ELCV0B002_PARAMS_QUERY001 */
    private static final String ELCV0B002_PARAMS_QUERY001 = "ELCV0B002_Params_Q001.sql";

    @Override
    public void beforeStep(StepExecution stepExecution) {
        String dataDate = stepExecution.getJobParameters().getString("dataDate");

        try {
            // Header 查詢
            Map<String, Object> headerMap = new HashMap<>();
            headerMap.put("DataDate", dataDate);
            List<Map<String, Object>> headerList = sqlAction.queryForList(sqlUtils.getQuerySql(ELCV0B002_HEADER_QUERY001), headerMap);

            ELCV0B002ExportHeaderVo headerVo;
            if (CollectionUtils.isEmpty(headerList)) {
                // 創建預設 headerVo
                headerVo = new ELCV0B002ExportHeaderVo();
                headerVo.setDataCnt("0");
                headerVo.setDataType("002");
                headerVo.setSystemId("LIP_VN");
                headerVo.setDataDate(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
                headerVo.setAcctDate("        ");
                headerVo.setCompanyCode("0604");
                headerVo.setBookCategory("1");
            } else {
                headerVo = mapHeader(headerList.get(0));
            }
            // 放進ExecutionContext，給Writer的headerCallback
            stepExecution.getExecutionContext().put("ELCV0B002_HEADER", headerVo);

            // 匯出參數查詢
            Map<String, Object> paramsMap = new HashMap<>();
            paramsMap.put("PROGRAM", "ELCV0_B002");
            List<Map<String, Object>> paramsList = sqlAction.queryForList(sqlUtils.getQuerySql(ELCV0B002_PARAMS_QUERY001), paramsMap);

            if (CollectionUtils.isEmpty(paramsList)) {
                throw new IllegalStateException("ELCV0B002 Params 查無資料");
            }

            ELCV0B002ExportParamsVo exportParams = mapParams(paramsList.get(0));
            stepExecution.getExecutionContext().put("ELCV0B002_PARAMS", exportParams);
        } catch (IOException ex) {
            throw new IllegalStateException("讀取 SQL 失敗：" + ex.getMessage(), ex);
        } catch (Exception ex) {
            throw new IllegalStateException("beforeStep 發生例外：" + ex.getMessage(), ex);
        }

    }

    /**
     * 表頭映射
     * @param row
     * @return
     */
    private ELCV0B002ExportHeaderVo mapHeader(Map<String, Object> row) {
        ELCV0B002ExportHeaderVo vo = new ELCV0B002ExportHeaderVo();
        vo.setDataType(MapUtils.getString(row, "DATA_TYPE"));
        vo.setSystemId(MapUtils.getString(row, "SYSTEM_ID"));

        Timestamp ts = (Timestamp) row.get("UPD_TIME");
        String updTimeStr = ts != null ? ts.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")) : "";
        vo.setDataDate(updTimeStr);

        String dataCntStr = (row.get("DATA_CNT") != null) ? String.valueOf(row.get("DATA_CNT")) : "";
        vo.setDataCnt(dataCntStr);

        Date date = (Date) row.get("ACCT_DATE");
        String acctDateStr = dataCntStr != null ? DateTimeFormatter.ofPattern("yyyyMMdd").format(date.toLocalDate()) : "";
        vo.setAcctDate(acctDateStr);

        vo.setCompanyCode(MapUtils.getString(row, "COMPANY_CODE"));
        vo.setBookCategory(MapUtils.getString(row, "BOOK_CATEGORY"));
        return vo;
    }

    /**
     * 匯出參數映射
     * @param row
     * @return
     */
    private ELCV0B002ExportParamsVo mapParams(Map<String, Object> row) {
        ELCV0B002ExportParamsVo vo = new ELCV0B002ExportParamsVo();
        vo.setIoTableName(MapUtils.getString(row, "IOTABLE_NAME"));
        vo.setIoFileName(MapUtils.getString(row, "IOFILE_NAME"));
        vo.setFileEncode(MapUtils.getString(row, "FILE_ENCODE"));
        vo.setFlgName(MapUtils.getString(row, "FLG_NAME"));
        vo.setIoFileEOF(MapUtils.getString(row, "IOFILE_EOF"));
        vo.setFileExpPath(MapUtils.getString(row, "FILE_EXP_PATH"));
        vo.setFlgExpPath(MapUtils.getString(row, "FLG_EXP_PATH"));
        vo.setTempExpPath(MapUtils.getString(row, "TEMP_EXP_PATH"));

        return vo;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        ExecutionContext jobCtx = stepExecution.getJobExecution().getExecutionContext();
        ELCV0B002ExportParamsVo params = (ELCV0B002ExportParamsVo) stepExecution.getExecutionContext().get("ELCV0B002_PARAMS");
        if (params != null) {
            if (stepExecution.getStatus() == BatchStatus.FAILED) {
                safeDelete(Paths.get(params.getTempExpPath(), params.getIoFileName()));
            }
            jobCtx.put("ELCV0B002_PARAMS", params);
        }

        return stepExecution.getExitStatus();

    }
    
    /**
     * 刪檔
     * @param p
     */
    private void safeDelete(Path p) {
        if (p == null) return;
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignore) {
        }
    }

}

package cub.lip.bch.elc.processor;

import java.nio.charset.Charset;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import cub.lip.bch.elc.util.BytePadUtil;
import cub.lip.bch.elc.vo.ELCV0B002ExportBodyVo;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@StepScope
@Component
public class ELCV0B002ExportBodyProcessor implements ItemProcessor<ELCV0B002ExportBodyVo, ELCV0B002ExportBodyVo> {

    /** MS950 */
    private static final Charset MS950 = Charset.forName("MS950");

    /** DTF_DATE */
    private static final DateTimeFormatter DTF_DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Override
    public ELCV0B002ExportBodyVo process(ELCV0B002ExportBodyVo item) throws Exception {

        item.setDataType(BytePadUtil.defaultPad(item.getDataType(), 3));
        item.setSystemId(BytePadUtil.defaultPad(item.getSystemId(), 10));
        item.setJournalCategory(BytePadUtil.defaultPad(item.getJournalCategory(), 10));
        item.setProposeDeptCode(BytePadUtil.defaultPad(item.getProposeDeptCode(), 4));

        String acctDateStr = item.getAcctDate() != null ? item.getAcctDate().toLocalDate().format(DTF_DATE) : "";
        item.setAcctDateStr(BytePadUtil.defaultPad(acctDateStr, 8));

        item.setCollCcyCode(BytePadUtil.defaultPad(item.getCollCcyCode(), 4));
        item.setDebitCreditType(BytePadUtil.defaultPad(item.getDebitCreditType(), 2));

        String digits = item.getCollAmt().movePointRight(4).toPlainString();
        item.setCollAmtStr(BytePadUtil.defaultPad(StringUtils.leftPad(digits, 18, '0'), 18));

        item.setAcctCode1BookingUnit(BytePadUtil.defaultPad(item.getAcctCode1BookingUnit(), 10));
        item.setAcctCode2Group(BytePadUtil.defaultPad(item.getAcctCode2Group(), 10));
        item.setAcctCode3Subsidiary(BytePadUtil.defaultPad(item.getAcctCode3Subsidiary(), 10));
        item.setAcctCode4ProductCode(BytePadUtil.defaultPad(item.getAcctCode4ProductCode(), 10));
        item.setAcctCode5ProjectCode(BytePadUtil.defaultPad(item.getAcctCode5ProjectCode(), 10));
        item.setAcctCode6Classification(BytePadUtil.defaultPad(item.getAcctCode6Classification(), 10));
        item.setEntryName(BytePadUtil.defaultPad("", 40));
        item.setEntryDesc(BytePadUtil.defaultPad("", 40));
        item.setPrintedFlag(BytePadUtil.defaultPad("Y", 1));
        item.setReserved1(BytePadUtil.defaultPad("", 9));
        item.setReserved2(BytePadUtil.defaultPad("", 10));
        item.setCompanyCode(BytePadUtil.defaultPad(item.getCompanyCode(), 4));
        item.setBookCategory(BytePadUtil.defaultPad(item.getBookCategory(), 1));

        String txDateStr = item.getTxDate() != null ? item.getTxDate().toLocalDate().format(DTF_DATE) : "";
        item.setTxDateStr(BytePadUtil.defaultPad(txDateStr, 8));
        item.setAcctCode7Filler(BytePadUtil.defaultPad(
            StringUtils.isBlank(item.getAcctCode7Filler()) ? "0" : item.getAcctCode7Filler(), 10));
        item.setFxRate(BytePadUtil.defaultPad("", 15));
        item.setFunctionalAmt(BytePadUtil.defaultPad("", 18));
        item.setExtraDesc(BytePadUtil.defaultPad("", 80));

        return item;
    }

}

package cub.lip.bch.elc.tasklet;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import cub.lip.bch.elc.entity.ElcAssetOffBlAcctF002;
import cub.lip.bch.elc.repository.ElcAssetOffBlAcctF002Repo;
import cub.lip.lib.common.util.sql.SqlAction;
import cub.lip.lib.common.util.sql.SqlUtils;
import lombok.extern.slf4j.Slf4j;

@Component
@StepScope
@Slf4j
public class ELCV0B002ImportDbTasklet implements Tasklet {

    /** SqlAction */
    @Autowired
    private SqlAction sqlAction;

    /** SqlUtils */
    @Autowired
    private SqlUtils sqlUtils;

    /** ObjectMapper */
    @Autowired
    private ObjectMapper objectMapper;

    /** ElcAssetOffBlAcctF002Repo */
    @Autowired
    private ElcAssetOffBlAcctF002Repo elcAssetOffBlAcctF002Repo;

    /** ELCV0B002_QUERY002 */
    private static final String ELCV0B002_QUERY001 = "ELCV0B002_Query_Q001.sql";

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        LocalDateTime localDateTime = LocalDateTime.now();
        Timestamp timestamp = Timestamp.valueOf(localDateTime);

        String dataDateStr = (String) chunkContext.getStepContext().getJobParameters().get("dataDate");
        Date dataDate = Date.valueOf(LocalDate.parse(dataDateStr, DateTimeFormatter.BASIC_ISO_DATE));

        String jobName = (String) chunkContext.getStepContext().getJobParameters().get("jobName");
        String empId = (String) chunkContext.getStepContext().getJobParameters().get("empId");

        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("PROGRAM", jobName);

        // 刪除當日ELC_ASSET_OFF_BL_ACCT_F002 暫存檔資料
        int deletedCount = elcAssetOffBlAcctF002Repo.deleteByDataDate(dataDate);
        log.info("ELC_ASSET_OFF_BL_ACCT_F002 刪除筆數: {}", deletedCount);

        // 將查詢結果放入$OffBalanceF002
        Map<String, Object> offBalanceParamMap = new HashMap<>();
        offBalanceParamMap.put("DataDate", dataDateStr);
        List<Map<String, Object>> offBalanceF002 = sqlAction.queryForList(sqlUtils.getQuerySql(ELCV0B002_QUERY001), offBalanceParamMap);

        if (!CollectionUtils.isEmpty(offBalanceF002)) {
            List<ElcAssetOffBlAcctF002> result = offBalanceF002.stream().map(map -> {
                ElcAssetOffBlAcctF002 elcAssetOffBlAcctF002 = objectMapper.convertValue(map, ElcAssetOffBlAcctF002.class);
                elcAssetOffBlAcctF002.setDataDate(dataDate);
                elcAssetOffBlAcctF002.setUpdId(empId);
                elcAssetOffBlAcctF002.setUpdTime(timestamp);
                return elcAssetOffBlAcctF002;
            }).collect(Collectors.toList());

            elcAssetOffBlAcctF002Repo.saveAll(result);
            log.info("ELC_ASSET_OFF_BL_ACCT_F002 新增筆數: {}", result.size());
        }

        return RepeatStatus.FINISHED;
    }

}

package cub.lip.bch.elc.vo;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;

import lombok.Data;

@Data
public class ELCV0B002ExportBodyVo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 資料類別 */
    private String dataType;

    /** 系統別(資料來源系統代號) */
    private String systemId;

    /** 分錄類別 */
    private String journalCategory;

    /** 起帳單位 */
    private String proposeDeptCode;

    /** 會計帳務日 */
    private Date acctDate;

    /** 會計帳務日 - 字串型別 */
    private String acctDateStr;

    /** 幣別 */
    private String collCcyCode;

    /** 借貸別 */
    private String debitCreditType;

    /** 原幣金額 */
    private BigDecimal collAmt;

    /** 原幣金額 - 字串型別 */
    private String collAmtStr;

    /** 科目節段1-記帳單位+帳簿 */
    private String acctCode1BookingUnit;

    /** 科目節段2-組別或成本中心 */
    private String acctCode2Group;

    /** 科目節段3--會計科子細目 */
    private String acctCode3Subsidiary;

    /** 科目節段4-產品別 */
    private String acctCode4ProductCode;

    /** 科目節段5-專案別 */
    private String acctCode5ProjectCode;

    /** 科目節段6-財務分類 */
    private String acctCode6Classification;

    /** 分錄名稱 */
    private String entryName;

    /** 分錄說明 */
    private String entryDesc;

    /** 傳票是否已於前端系統列印 */
    private String printedFlag;

    /** 保留欄位一 */
    private String reserved1;

    /** 保留欄位二 */
    private String reserved2;

    /** 公司別 */
    private String companyCode;

    /** 帳冊別 */
    private String bookCategory;

    /** 交易日 */
    private Date txDate;

    /** 交易日 - 字串型別 */
    private String txDateStr;

    /** 科目節段7-保留一 */
    private String acctCode7Filler;

    /** 匯率 */
    private String fxRate;

    /** 功能性貨幣金額 */
    private String functionalAmt;

    /** 附加說明 */
    private String extraDesc;

}

package cub.lip.bch.elc.vo;

import java.io.Serializable;

import lombok.Data;

@Data
public class ELCV0B002ExportHeaderVo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 資料類別 */
    private String dataType;

    /** 系統別 */
    private String systemId;

    /** 資料日期時間 */
    private String dataDate;

    /** 資料筆數 */
    private String dataCnt;

    /** 帳務日 */
    private String acctDate;

    /** 公司別 */
    private String companyCode;

    /** 帳冊別 */
    private String bookCategory;
}

package cub.lip.bch.elc.vo;

import java.io.Serializable;

import lombok.Data;

@Data
public class ELCV0B002ExportParamsVo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 匯入/匯出表格名稱 */
    private String ioTableName;

    /** 匯入/匯出檔名 */
    private String ioFileName;

    /** 檔案編碼 */
    private String fileEncode;

    /** 匯出flg 檔名 */
    private String flgName;

    /** 資料行結束符號 */
    private String ioFileEOF;

    /** 檔案輸出路徑 */
    private String fileExpPath;

    /** flg檔輸出路徑 */
    private String flgExpPath;

    /** 檔案暫存路徑 */
    private String tempExpPath;

}
