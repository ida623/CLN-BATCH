package cub.lip.svc.pln.batch.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;

import cub.lip.lib.common.exception.DataNotFoundException;
import cub.lip.lib.common.util.sql.SqlUtils;
import cub.lip.svc.batch.context.DbToFileContext;
import cub.lip.svc.batch.flow.DbToFileFlowConfig;
import cub.lip.svc.batch.flow.PreBatchCheckFlowConfig;
import cub.lip.svc.batch.listener.SimpleJobListener;
import cub.lip.svc.batch.vo.DbToFileVO;
import cub.lip.svc.pln.batch.context.FilePathContext;
import cub.lip.svc.pln.batch.entity.PlnJcic207IdTemp;
import cub.lip.svc.pln.batch.listener.PLNJ0B002ImportTmpDbListener;
import cub.lip.svc.pln.batch.listener.PLNJ0B002Jcic207UpdateListener;
import cub.lip.svc.pln.batch.processor.PLNJ0B002CreateJCIC207Processor;
import cub.lip.svc.pln.batch.processor.PLNJ0B002ImportTmpDbProcessor;
import cub.lip.svc.pln.batch.repository.PlnJcic207IdTempRepo;
import cub.lip.svc.pln.batch.service.ExportFlgSvc;
import cub.lip.svc.pln.batch.util.BaseServiceUtil;
import cub.lip.svc.pln.batch.vo.PLNJ0B002CustomerInfoVo;
import cub.lip.svc.pln.batch.vo.PLNJ0B002Jcic207ImportVo;
import cub.lip.svc.pln.batch.vo.PLNJ0B002Jcic207UpdateVo;
import lombok.extern.slf4j.Slf4j;

/**
 * PLNJ0_B002 聯徵207報送批次服務 JobConfig 
 * @author 00590779
 *
 */
@Configuration
@Slf4j
public class PLNJ0B002JobConfig {

    /** DataSource */
    @Autowired
    private DataSource dataSource;

    /**JdbcTemplate*/
    @Autowired
    private JdbcTemplate jdbcTemplate;

    /** JobBuilderFactory */
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    /** StepBuilderFactory */
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    /** PlnPreBatchCheckFlowConfig */
    @Autowired
    private PreBatchCheckFlowConfig preBatchCheck;

    /**DbToFileContext*/
    @Autowired
    private DbToFileContext jobContext;

    /** FilePathContext */
    @Autowired
    private FilePathContext pathContext;

    /**PlnJcic207IdTempRepo*/
    @Autowired
    private PlnJcic207IdTempRepo plnJcic207IdTempRepo;
    
    /** SqlUtils */
    @Autowired
    private SqlUtils sqlUtils;
    
    /**BaseServiceUtil*/
    @Autowired
    private BaseServiceUtil baseServiceUtil;

    /** DbToFileFlowConfig */
    @Autowired
    private DbToFileFlowConfig dbToFileflowCongfig;

    /** SqlUtils */
    @Autowired
    @Qualifier("PLNJ0B002pagingQueryProvider")
    private PagingQueryProvider provider;

    /** importPath */
    @Value("${lip.import.path}")
    private String importPath;

    /** 每個chuck的數量 */
    @Value("${lip.batch.fetch-size}")
    private int fetchSize;

    /** 備份路徑(pilot路徑) */
    @Value("${lip.export.backup.path}")
    private String exportBackupPath;

    /**ODS匯出檔案暫存路徑*/
    @Value("${lip.export.temp.path}")
    private String tempPath;

    /** ODS匯出檔案路徑 */
    @Value("${lip.export.ODS.path}")
    private String exportPath;

    /** FLG Init 暫存路徑 */
    @Value("${lip.export.flgInit.path}")
    private String exportFlgInitPath;

    /** PLNJ0B002_findCustomerInfo_Q001 */
    private static final String SQL_Q001 = "PLNJ0B002_findCustomerInfo_Q001.sql";

    /**MAPPER_FIELD*/
    private static final String[] MAPPER_FIELD = new String[] { "customerId" };
    
    /** PLNJ0B002 Job */
    @Bean
    public Job PLNJ0B002Job(SimpleJobListener simpleJobListener) throws Exception {
        DbToFileVO dbToFileVO = new DbToFileVO();
        dbToFileVO.setExportPath(
            "Y".equals(baseServiceUtil.getSystemParam("SYSTEM", "system.pilotrun.flag")) ? exportBackupPath : exportPath);
        dbToFileVO.setExportFlgPath(exportFlgInitPath);
        dbToFileVO.setExportTempPath(tempPath);

        String fileName = importPath + File.separator + "JCIC080_LONLXLIP01.flg";
        return jobBuilderFactory.get("PLNJ0_B002")
                .start(preBatchCheck.preBatchCheckFlow())
                .next(dbToFileflowCongfig.dbToFileInitContextStep(dbToFileVO))
                .next(checkFlgFileDecider()).on("Y") // 判斷control檔
                    .to(getFlgContentsStep(fileName)) // 開啟flg檔取得字串: "JCIC080_LONLXLIP01{YYYYMMDD}.txt"
                    .next(PLNJ0B002clearTempDbStep())
                    .next(PLNJ0B002ImportTmpDbStep(null, null))
                    .next(PLNJ0B002BackupFileStep())
                    .next(PLNJ0B002CreateJCIC207Step(null, null))
                .from(checkFlgFileDecider()).on("*").end()
                .from(PLNJ0B002CreateJCIC207Step(null,null)).on("COMPLETED")
                	.to(PLNJ0B002ExportFlgStep(null))    
                	.next(dbToFileflowCongfig.moveFileStep(null))
                .from(PLNJ0B002CreateJCIC207Step(null, null)).on("*")
                    .to(deleteFileWhenFailStep())
                .end()
                .listener(simpleJobListener)
                .build();
    }
    
    /**
     * 判斷control檔(JCIC080_LONLXLIP01.flg)存在且有資料
     * @return
     */
    @Bean
    public JobExecutionDecider checkFlgFileDecider() {
        return (jobExecution, stepExecution) -> {
            File flgFile = new File(importPath, "JCIC080_LONLXLIP01.flg");
            if (!flgFile.exists()) {
                log.debug("control檔(JCIC080_LONLXLIP01.flg)不存在");
                return new FlowExecutionStatus("N");
            } else if (flgFile.length() < 0) {
                log.debug("control檔(JCIC080_LONLXLIP01.flg)沒有資料");
                return new FlowExecutionStatus("N");
            }
            return new FlowExecutionStatus("Y");
        };
    }

    /**
     * 取得flg檔內容
     * @return
     */
    @Bean
    public Step getFlgContentsStep(String fileName) {
        return stepBuilderFactory.get("getFlgContentsStep")
                .tasklet((contribution, chunkContext) -> {
                   FileReader fr = new FileReader(fileName);
                   BufferedReader br = new BufferedReader(fr);
                   while(br.ready()) {
                       pathContext.setFlgContent(br.readLine());
                   }
                   fr.close();                 
                   return RepeatStatus.FINISHED;
                })
                
                .build();
    }
    
    /**
     * 備份檔案
     * @param tasklet
     * @return
     */
    @Bean
    public Step PLNJ0B002BackupFileStep() {
    	return stepBuilderFactory.get("PLNJ0B002BackupFileStep")
                .tasklet((contribution, chunkContext) -> {
                	String flgName = importPath + File.separator + "JCIC080_LONLXLIP01.flg";
                	String txtName = importPath + File.separator + pathContext.getFlgContent();
                	Path importFlg = Paths.get(flgName);
                	Path importTxt = Paths.get(txtName);
                	if (importFlg.toFile().exists() && importTxt.toFile().exists()) {
                		Path newFlg = Paths.get(importPath  + "/backup/", "JCIC080_LONLXLIP01.flg");
                		Path newTxt = Paths.get(importPath  + "/backup/", pathContext.getFlgContent());
                	    Files.move(importFlg, newFlg, StandardCopyOption.REPLACE_EXISTING);
                	    Files.move(importTxt, newTxt, StandardCopyOption.REPLACE_EXISTING);
                	} else {
                		log.info("[{}] 無Flg檔 - {} 或無Txt檔 - {}", "PLNJ0_B002", flgName, txtName);
                	}         
                   return RepeatStatus.FINISHED;
                })
                .build();
    }
    
//    /**
//     * 判斷txt內容的身分證是否合法
//     * @param tasklet
//     * @return
//     */
//    @Bean 
//    public Step assortLegalCustomerIdStep(PLNJ0B002AssortLegalCustomerIdTasklet tasklet) {
//        return stepBuilderFactory.get("assortLegalCustomerIdStep")
//                .tasklet(tasklet)
//                .build();
//    }   

    /**
     * Create JCIC207.txt step
     * @param processor
     * @return
     * @throws Exception
     */
    @Bean
    public Step PLNJ0B002CreateJCIC207Step(PLNJ0B002CreateJCIC207Processor processor, PLNJ0B002Jcic207UpdateListener listener) throws Exception {
        return stepBuilderFactory.get("PLNJ0B002CreateJCIC207Step")
                .<PLNJ0B002CustomerInfoVo, PLNJ0B002Jcic207UpdateVo>chunk(fetchSize)
                .reader(PLNJ0B002CreateJCIC207Reader())
                .processor(processor)
                .writer(PLNJ0B002CreateJCIC207Writer())
                .listener(listener)
                .build();
    }
    
    /**
     * 輸出Flg並移動檔案
     * @param exportFlgSvc
     * @return
     * @throws DataNotFoundException 
     */
    @Bean
    public Step PLNJ0B002ExportFlgStep(ExportFlgSvc exportFlgSvc) {

        return stepBuilderFactory.get("PLNJ0B002ExportFlgStep")
                .tasklet((contribution, chunkContext) -> {
                    exportFlgSvc.doExportFlg("LONLXLIP01_JCIC207_UPLOAD.flg", "LONLXLIP01_JCIC207_UPLOAD.txt", jobContext.getExportPath(),
                        jobContext.getExportFlgPath());
                    return RepeatStatus.FINISHED;
                })
                .build();
    }
    
    /**
     * PLNJ0B002CreateJCIC207Reader
     * @return
     * @throws Exception
     */
    @Bean
    @StepScope
    public JdbcPagingItemReader<PLNJ0B002CustomerInfoVo> PLNJ0B002CreateJCIC207Reader() throws Exception {
        return new JdbcPagingItemReaderBuilder<PLNJ0B002CustomerInfoVo>()
                .name("PLNJ0B002CreateJCIC207Reader")
                .dataSource(dataSource)
                .queryProvider(provider) // sql
                .pageSize(100) // 單次查詢?筆資料
                .beanRowMapper(PLNJ0B002CustomerInfoVo.class)
                .build();
    }
    
    @Bean
    public Step deleteFileWhenFailStep() {
        return stepBuilderFactory.get("deleteFileWhenFailStep")
                .tasklet((contribution, chunkContext) -> {
                    Files.deleteIfExists(Paths.get(jobContext.getExportFlgPath(),"LONLXLIP01_JCIC207_UPLOAD.flg"));
                    Files.deleteIfExists(Paths.get(jobContext.getExportPath(),"LONLXLIP01_JCIC207_UPLOAD.flg"));
                    Files.deleteIfExists(Paths.get(jobContext.getExportPath(),"LONLXLIP01_JCIC207_UPLOAD.txt"));
                    return RepeatStatus.FINISHED;
                })
                .build();
    }
    
    /**
     * PLNJ0B002CreateJCIC207Writer
     * @return
     * @throws DataNotFoundException 
     */
    @Bean
    @StepScope
    public FlatFileItemWriter<PLNJ0B002Jcic207UpdateVo> PLNJ0B002CreateJCIC207Writer() {
        String[] args = {"transCode", "officeCode", "space1", "dataDate", "customerId", "customerName", "englishName", "birthday", "regAddress", "zipCode", 
                "conAddress", "homePhone", "cellPhone", "space2", "educationCode", "hasOwnHome", "jobCompany", "jobCompanyUniformNum", 
                "jobCode", "jobTel", "jobName", "workYear", "yearSalary", "salaryDate", "sex", "foreignerFlag", "passportId", "oldTaxNum", 
                "nameLengthOverTen","space3"};
        jobContext.setIoFile(Paths.get(tempPath, "LONLXLIP01_JCIC207_UPLOAD.txt"));
        return new FlatFileItemWriterBuilder<PLNJ0B002Jcic207UpdateVo>()
                .name("PLNJ0B002CreateJCIC207Writer")
                .resource(new FileSystemResource(jobContext.getIoFile()))
                .shouldDeleteIfExists(true)
                .transactional(true)
                .encoding("MS950")
                .delimited()
                .delimiter("")
                .names(args)
                .build();
    }
    
    /**
     * 分頁方式讀取DB資料
     * @param dataSource
     * @return
     * @throws IOException
     */
    @Bean
    @Qualifier("PLNJ0B002pagingQueryProvider")
    public SqlPagingQueryProviderFactoryBean PLNJ0B002pagingQueryProvider(DataSource dataSource) throws IOException {
        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("CUSTOMER_ID", Order.DESCENDING);
        sortKeys.put("CUSTOMER_NAME", Order.DESCENDING);
        String[] sqlSplit = sqlUtils.getQuerySql(SQL_Q001).split("from",2);
        
        SqlPagingQueryProviderFactoryBean factoryBean = new SqlPagingQueryProviderFactoryBean();
        factoryBean.setDataSource(dataSource);
        factoryBean.setSelectClause(sqlSplit[0]);
        factoryBean.setFromClause(" from" + sqlSplit[1]);
        factoryBean.setSortKeys(sortKeys);
        return factoryBean;
    }
    
    /**
     * PLNJ0B002ImportTmpDbStep
     * @param processor
     * @param listener
     * @return
     */
    @Bean
    public Step PLNJ0B002ImportTmpDbStep(PLNJ0B002ImportTmpDbProcessor processor,PLNJ0B002ImportTmpDbListener listener) {
        return stepBuilderFactory.get("PLNJ0B002ImportTmpDb")
                .<PLNJ0B002Jcic207ImportVo,PlnJcic207IdTemp>chunk(fetchSize)
                .reader(PLNJ0B002ImportTmpDbReader())
                .processor(processor)
                .writer(PLNJ0B002ImportTmpDbWriter())
                .listener(listener)
                .build();
        
    }
    
    /**
     * 清空temp table
     * @return
     */
    @Bean
    public Step PLNJ0B002clearTempDbStep() {
        return this.stepBuilderFactory.get("PLNJ0B002clearTempDbStep").tasklet((contribution, chunkContext) -> {
            jdbcTemplate.update("truncate table LONLXLIP01PLN.PLN_JCIC207_ID_TEMP");
            return RepeatStatus.FINISHED;
        }).build();
    }
    
    /**
     * PLNJ0B002ImportTmpDbReader
     * @return
     */
    @Bean
    @StepScope
    public MultiResourceItemReader<PLNJ0B002Jcic207ImportVo> PLNJ0B002ImportTmpDbReader() {
        FlatFileItemReader<PLNJ0B002Jcic207ImportVo> itemReader = new FlatFileItemReaderBuilder<PLNJ0B002Jcic207ImportVo>()
                .name("PLNJ0B002ImportTmpDbReader")
                .encoding("UTF-8")
                .lineTokenizer(getLineTokenizer())
                .targetType(PLNJ0B002Jcic207ImportVo.class)
                .build();
        
        return new MultiResourceItemReaderBuilder<PLNJ0B002Jcic207ImportVo>()
                .name("PLNJ0B002ImportTmpDbMultiReader")
                .delegate(itemReader)
                .resources(new FileSystemResource(new File(pathContext.getImportPath(), pathContext.getFlgContent())))
                .build();
    }
    
    /**
     * PLNJ0B002ImportTmpDbWriter
     * @return
     */
    public ItemWriter<PlnJcic207IdTemp> PLNJ0B002ImportTmpDbWriter(){
        return new RepositoryItemWriterBuilder<PlnJcic207IdTemp>()
                .repository(plnJcic207IdTempRepo)
                .methodName("save")
                .build();
    }
    
    /**
     * getLineTokenizer
     * @return
     */
    private LineTokenizer getLineTokenizer() {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setNames(MAPPER_FIELD);
        return delimitedLineTokenizer;
    }
}

package cub.lip.svc.pln.batch.listener;

import java.util.List;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@StepScope
@Slf4j
public class PLNJ0B002ImportTmpDbListener {

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        String jobName = stepExecution.getJobExecution().getJobInstance().getJobName();
        log.info("{}: 寫入PLN_JCIC207_ID_TEMP開始", jobName);
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        String jobName = stepExecution.getJobExecution().getJobInstance().getJobName();

        List<Throwable> expList = stepExecution.getFailureExceptions();
        if (!expList.isEmpty()) {
            expList.stream().forEach(exp -> log.error("{}: ", jobName, exp));
            return ExitStatus.FAILED;
        }

        log.info("{}: 寫入PLN_JCIC207_ID_TEMP完成", jobName);
        return ExitStatus.COMPLETED;
    }
}

package cub.lip.svc.pln.batch.listener;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import cub.lip.svc.pln.batch.context.FilePathContext;
import cub.lip.svc.pln.batch.context.PLNJ0B002Context;
import lombok.extern.slf4j.Slf4j;

/**
 * PLNJ0B002Jcic207Update Listener
 * @author 00590779
 *
 */
@Slf4j
@Component
@StepScope
public class PLNJ0B002Jcic207UpdateListener {

    /** PLNJ0B002Context */
    @Autowired
    private PLNJ0B002Context jobContext;

    /** FilePathContext */
    @Autowired
    private FilePathContext pathContext;

    /**
     * after PLNJ0B002Jcic207UpdateStep: 寫入失敗身分證ID 及 log印出完成筆數
     * @param stepExecution
     * @return
     * @throws IOException
     */
    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) throws Exception {
        StringBuilder sb = new StringBuilder();
        List<String> failCustomerIDList = jobContext.getFailCustomerIDList();
        if (!CollectionUtils.isEmpty(failCustomerIDList)) {

            String errPath = pathContext.getExportErrPath();
            Path flgPath = Paths.get(errPath);
            if (Files.notExists(flgPath)) {
                Files.createDirectories(flgPath);
            }
            try (FileWriter fw = new FileWriter(new File(errPath + File.separator + "LONLXLIP01_JCIC207_ERROR_ID.txt"));
                    PrintWriter printWriter = new PrintWriter(fw)) {

                //寫入失敗身分證字號
                for (String failId : failCustomerIDList) {
                    printWriter.println(failId);
                }
            } catch (IOException e) {
                log.error("LONLXLIP01_JCIC207_ERROR_ID.txt 產製失敗...");
                e.printStackTrace();
            }
        }
        sb.setLength(0);
        String logInfo = sb.append("PLNJ0_B002-Total Counts:").append(jobContext.getTotalCount()).append(". Success Counts:")
                .append(jobContext.getSuccessCount()).append(". Fail Counts:").append(jobContext.getFailCount()).toString();
        
        log.info(logInfo);
        return ExitStatus.COMPLETED;
    }
}

package cub.lip.svc.pln.batch.processor;

import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.chrono.MinguoDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import cub.lip.svc.pln.batch.context.PLNJ0B002Context;
import cub.lip.svc.pln.batch.vo.PLNJ0B002CustomerInfoVo;
import cub.lip.svc.pln.batch.vo.PLNJ0B002Jcic207UpdateVo;
import lombok.extern.slf4j.Slf4j;

/**
 * PLNJ0B002 CreateJCIC207 Processor
 * 
 * @author 00590779
 *
 */
@Component
@StepScope
@Slf4j
public class PLNJ0B002CreateJCIC207Processor
		implements ItemProcessor<PLNJ0B002CustomerInfoVo, PLNJ0B002Jcic207UpdateVo> {

	/** PLNJ0B002Context */
	@Autowired
	private PLNJ0B002Context context;

	/** 日期格式 */
	private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	/** 民國格式 */
	private static final DateTimeFormatter MINGUO_DATE = DateTimeFormatter.ofPattern("yyyMMdd");

	/**
	 * 全形空格的值，它沒有遵從與ASCII的相對位移，必須單獨處理
	 */
	static final char SBC_SPACE = 12288; // 全形空格 12288
	/**
	 * 半形空格的值，在ASCII中為32(Decimal)
	 */
	static final char DBC_SPACE = ' '; // 半形空格

	/**
	 * ASCII表中可見字元從!開始，位移位值為33(Decimal)
	 */
	static final char DBC_CHAR_START = 33; // 半形!
	/**
	 * ASCII表中可見字元到~結束，位移位值為126(Decimal)
	 */
	static final char DBC_CHAR_END = 126; // 半形~

	/**
	 * ASCII表中除空格外的可見字元與對應的全形字元的相對位移
	 */
	static final int CONVERT_STEP = 65248; // 全形半形轉換間隔

	@Override
	public PLNJ0B002Jcic207UpdateVo process(PLNJ0B002CustomerInfoVo vo) throws Exception {

		if (checkVoIfExist(vo)) {
			PLNJ0B002Jcic207UpdateVo jcic207 = new PLNJ0B002Jcic207UpdateVo();
			Charset charset = Charset.forName("MS950");
			jcic207.setTransCode("A");
			jcic207.setOfficeCode("013");
			jcic207.setSpace1(fillBlank("", 4, charset));
			jcic207.setDataDate(MinguoDate.now().format(MINGUO_DATE));
			jcic207.setCustomerId(fillBlank(vo.getCustomerId(), 10, charset));

			String customerName = trimNull(vo.getCustomerName());
			String fullCName = toFullWidth(customerName);
			if (fullCName.length() > 10) {
				fullCName = fullCName.substring(0, 10);
			}
			jcic207.setCustomerName(fillBlank(fullCName, 20, charset)); // 全形 中文姓名
			jcic207.setEnglishName(fillBlank("", 20, charset)); // 英文姓名
			String minguoBirthday = MinguoDate.from(LocalDate.parse(vo.getBirthday(), DTF)).format(MINGUO_DATE);
			jcic207.setBirthday(minguoBirthday); // 出生日期

			String regAddress = trimNull(vo.getRegAddress());
			jcic207.setRegAddress(fillBlank(toFullWidth(regAddress), 66, charset)); // 全形 戶籍地址

			jcic207.setZipCode(fillBlank(trimNull(vo.getZipCode()), 5, charset)); // 聯絡地址郵遞區號

			String conAddress = trimNull(vo.getConAddress());
			jcic207.setConAddress(fillBlank(trimNull(toFullWidth(conAddress)), 66, charset)); // 全形 聯絡地址

			String homePhone = StringUtils.isNotBlank(vo.getHomePhone()) ? vo.getConHome() : vo.getCellPhone();
			jcic207.setHomePhone(fillBlank(trimNull(homePhone), 16, charset)); // 聯絡電話
			
			jcic207.setCellPhone(fillBlank(trimNull(vo.getCellPhone()), 16, charset)); // 行動電話
			
			jcic207.setSpace2(fillBlank("", 5, charset)); 
			jcic207.setEducationCode(fillBlank(trimNull(vo.getEducationCode()), 1, charset)); // 教育程度代號
			
			jcic207.setHasOwnHome(fillBlank("", 1, charset)); // 自有住宅有無
			
			jcic207.setJobCompany(fillBlank("", 30, charset)); // 全形 任職機構名稱
			
			jcic207.setJobCompanyUniformNum(fillBlank("", 8, charset)); // 任職機構統一編號
			jcic207.setJobCode(fillBlank(vo.getJobCode(), 6, charset)); // 職業類別
			
			jcic207.setJobTel(fillBlank("", 16, charset)); // 任職機構電話
			
			jcic207.setJobName(fillBlank("", 10, charset)); // 職位名稱
			
			jcic207.setWorkYear(StringUtils.leftPad(vo.getWorkYear(), 2, "0")); // 服務年資
			
			jcic207.setYearSalary(
					StringUtils.leftPad(vo.getYearSalary().setScale(0, RoundingMode.FLOOR).toPlainString(), 6, "0")); // 年收入(千元)
			jcic207.setSalaryDate(fillBlank(vo.getSalaryDate(), 5, charset)); // 年收入資料年月
			
			jcic207.setSex(fillBlank(vo.getSex(), 1, charset)); // 性別
			
			jcic207.setForeignerFlag(fillBlank("", 2, charset)); // 國籍
			
			jcic207.setPassportId(fillBlank(trimNull(vo.getPassportId()), 20, charset)); // 護照號碼
			
			jcic207.setOldTaxNum(fillBlank("", 10, charset)); // 舊有稅籍編號
			
			// 中文姓名超過10個字的全名
			if (toFullWidth(customerName).length() > 10) {
				jcic207.setNameLengthOverTen(fillBlank(toFullWidth(customerName), 200, charset));
			} else {
				jcic207.setNameLengthOverTen(fillBlank("", 200, charset));
			}
			jcic207.setSpace3(fillBlank("", 36, charset));
			return jcic207;
		}
		context.setFailCustomerID(vo.getCustomerId());
		return null;
	}

	/**
	 * 檢核報送資料是否都有值
	 * 
	 * @param vo
	 * @return
	 * @throws Exception
	 */
	private boolean checkVoIfExist(PLNJ0B002CustomerInfoVo vo) throws Exception {

		boolean notPattern = !Pattern.matches("^[A-Z][12][0-9]{8}", vo.getCustomerId());
		if (StringUtils.isBlank(vo.getCustomerName()) || StringUtils.isBlank(vo.getBirthday())
				|| StringUtils.isBlank(vo.getRegAddress()) || StringUtils.isBlank(vo.getConAddress())
				|| StringUtils.isBlank(vo.getJobCode()) || StringUtils.isBlank(vo.getYearSalary().toPlainString())
				|| StringUtils.isBlank(vo.getSex())
				|| (notPattern
						&& (StringUtils.isBlank(vo.getForeignerFlag())
								|| StringUtils.equals("N", vo.getForeignerFlag()))
						&& StringUtils.isBlank(vo.getPassportId()))
				|| (StringUtils.isBlank(vo.getConHome()) && StringUtils.isBlank(vo.getCellPhone()))) {
			log.info(vo.getCustomerId() + "的報送資料有空值");
			return false;
		}
		return true;
	}

	/**
	 * return "" if property is null
	 * 
	 * @param data
	 * @return
	 */
	private String trimNull(String property) {
		return StringUtils.isEmpty(property) ? "" : property;
	}

	/**
	 * toFullWidth
	 * 
	 * @param data
	 * @return
	 */
	private String toFullWidth(String str) {
		if (str == null) {
			return str;
		}
		StringBuilder buf = new StringBuilder(str.length());
		char[] ca = str.toCharArray();
		for (int i = 0; i < ca.length; i++) {
			if (ca[i] == DBC_SPACE) { // 如果是半形空格，直接用全形空格替代
				buf.append(SBC_SPACE);
			} else if ((ca[i] >= DBC_CHAR_START) && (ca[i] <= DBC_CHAR_END)) { // 字元是!到~之間的可見字元
				buf.append((char) (ca[i] + CONVERT_STEP));
			} else { // 不對空格以及ascii表中其他可見字元之外的字元做任何處理
				buf.append(ca[i]);
			}
		}
		return buf.toString();
	}

	/**
	 * fillBlank
	 * 
	 * @param str
	 * @param maxLen
	 * @param charset
	 * @return
	 */
	private String fillBlank(String str, int maxLen, Charset charset) {
		if (str == null) {
			return str;
		}
		if (charset == null) {
			charset = Charset.forName("MS950");
		}

		try {
			byte[] str_byte = str.getBytes(charset.name());
			int str_len = str_byte.length;
			if (str_len > maxLen) {

				// 超出長度的處理
				String[] strArray = str.split("");
				List<String> strList = Arrays.asList(strArray);
				int sumLength = 0;
				for (int i = 0; i < strList.size(); i++) {
					int oneStrLength = strList.get(i).getBytes(charset.name()).length;
					sumLength += oneStrLength;
					if (sumLength > maxLen) {
						str = str.substring(0, i);
						str_len = str.getBytes(charset.name()).length;
						break;
					}
				}
			}

			StringBuffer sbf = new StringBuffer();
			sbf.append(str);
			for (; str_len < maxLen; str_len++) {
				sbf.append(" ");
			}
			return sbf.toString();
		} catch (Exception e) {
			log.error("" + e);
			return str;
		}
	}

}

package cub.lip.svc.pln.batch.processor;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import cub.lip.svc.pln.batch.entity.PlnJcic207IdTemp;
import cub.lip.svc.pln.batch.vo.PLNJ0B002Jcic207ImportVo;

@Component
@StepScope
public class PLNJ0B002ImportTmpDbProcessor implements ItemProcessor<PLNJ0B002Jcic207ImportVo, PlnJcic207IdTemp> {

    @Override
    public PlnJcic207IdTemp process(PLNJ0B002Jcic207ImportVo item) throws Exception {

        PlnJcic207IdTemp plnJcic207IdTemp = new PlnJcic207IdTemp();
        String customerId = StringUtils.trim(StringUtils.remove(item.getCustomerId(), ";"));
        if (Pattern.matches("^[A-Z][12][0-9]{8}", customerId)) {
            plnJcic207IdTemp.setCustomerId(StringUtils.trim(StringUtils.remove(customerId, ";")));
            plnJcic207IdTemp.setUpdTime(Timestamp.valueOf(LocalDateTime.now()));
            return plnJcic207IdTemp;
        }
        return null;
    }

}
