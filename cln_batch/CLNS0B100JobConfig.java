package cub.lip.bch.cln.job;

import java.io.IOException;
import java.nio.file.Paths;

import javax.sql.DataSource;

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

import cub.lip.bch.cln.processor.CLNS0B100ExportBodyProcessor;
import cub.lip.bch.cln.tasklet.CLNS0B100BackupTasklet;
import cub.lip.bch.cln.vo.CLNS0B100ExportBodyVo;
import cub.lip.lib.common.util.sql.SqlUtils;
import cub.lip.svc.batch.context.DbToFileContext;
import cub.lip.svc.batch.enums.FileToDbTargetEnum;
import cub.lip.svc.batch.flow.FileToDbFlowConfig;
import cub.lip.svc.batch.flow.PreBatchCheckFlowConfig;
import cub.lip.svc.batch.listener.SimpleJobListener;
import cub.lip.svc.batch.vo.FileToDbVO;

@Configuration
public class CLNS0B100JobConfig {

    @Value("${lip.batch.fetch-size}")
    private int fetchSize;

    @Value("${lip.import.path}")
    private String importPath;

    @Value("${lip.export.temp.path}")
    private String exportTempPath;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private PreBatchCheckFlowConfig preBatchCheck;

    @Autowired
    private FileToDbFlowConfig fileToDbFlowConfig;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private SqlUtils sqlUtils;

    /** CLNS0B100_QUERY_PERSON_DATA_Q001 */
    private static final String CLNS0B100_QUERY_PERSON_DATA_Q001 = "CLNS0B100_QueryPersonData_Q001.sql";

    /**
     * CLNS0_B100 主 Job：匯入名單資料 -> 匯出 JCIC207 檔案 -> 備份/搬移檔案
     * @param simpleJobListener
     * @param exportStep
     * @param backupStep
     * @return
     */
    @Bean
    public Job CLNS0B100Job(
            SimpleJobListener simpleJobListener,
            @Qualifier("CLNS0B100ExportStep") Step exportStep,
            @Qualifier("CLNS0B100BackupStep") Step backupStep
        ) {
        FileToDbVO fileToDbVO = new FileToDbVO();
        fileToDbVO.setImportPath(importPath);
        fileToDbVO.setClearBeforeImport("Y");
        fileToDbVO.setTableNameColumn(FileToDbTargetEnum.TMP_TABLE);

        return jobBuilderFactory.get("CLNS0_B100")
                .start(preBatchCheck.preBatchCheckFlow())
                .next(fileToDbFlowConfig.fileToDbFlow(fileToDbVO))
                .next(exportStep)
                .next(backupStep)
                .end()
                .listener(simpleJobListener)
                .build();
    }

    /**
     * 匯出 JCIC207 步驟：讀取查詢結果、轉換成固定長度格式、寫出檔案
     * @param CLNS0B100ExportReader
     * @param CLNS0B100BodyProcessor
     * @param CLNS0B100ExportWriter
     * @return
     */
    // TODO: 等 Processor 改成 ItemProcessor<CLNS0B100ExportBodyVo, CLNS0B100JCIC207ExportVo> 之後，
    // 這裡要跟著改：writer 參數型別改成 FlatFileItemWriter<CLNS0B100JCIC207ExportVo>，
    // .chunk() 的泛型改成 <CLNS0B100ExportBodyVo, CLNS0B100JCIC207ExportVo>，
    // 記得補 import cub.lip.bch.cln.vo.CLNS0B100JCIC207ExportVo
    @Bean
    public Step CLNS0B100ExportStep(
            JdbcCursorItemReader<CLNS0B100ExportBodyVo> CLNS0B100ExportReader,
            CLNS0B100ExportBodyProcessor CLNS0B100BodyProcessor,
            FlatFileItemWriter<CLNS0B100ExportBodyVo> CLNS0B100ExportWriter) {
        return stepBuilderFactory.get("CLNS0B100ExportStep")
                .<CLNS0B100ExportBodyVo, CLNS0B100ExportBodyVo>chunk(fetchSize)
                .reader(CLNS0B100ExportReader)
                .processor(CLNS0B100BodyProcessor)
                .writer(CLNS0B100ExportWriter)
                .build();
    }

    /**
     * 備份/搬移匯出檔案步驟
     * @param tasklet
     * @return
     */
    @Bean
    public Step CLNS0B100BackupStep(CLNS0B100BackupTasklet tasklet) {
        return stepBuilderFactory.get("CLNS0B100BackupStep")
                .tasklet(tasklet)
                .build();
    }

    /**
     * 查詢待匯出人員資料的 Reader
     * @return
     * @throws IOException
     */
    @Bean
    @StepScope
    public JdbcCursorItemReader<CLNS0B100ExportBodyVo> CLNS0B100ExportReader() throws IOException {
        JdbcCursorItemReader<CLNS0B100ExportBodyVo> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql(sqlUtils.getQuerySql(CLNS0B100_QUERY_PERSON_DATA_Q001));
        reader.setRowMapper(new BeanPropertyRowMapper<>(CLNS0B100ExportBodyVo.class));
        return reader;
    }

    /**
     * 輸出 JCIC207 固定長度格式檔案的 Writer
     * @param jobContext
     * @return
     */
    // TODO: 泛型改成 FlatFileItemWriter<CLNS0B100JCIC207ExportVo>，
    // 下面 FlatFileItemWriterBuilder<CLNS0B100ExportBodyVo> 也要跟著改成 <CLNS0B100JCIC207ExportVo>；
    // names[] 陣列內容不用動，因為是照輸出格式欄位順序排的，新 Vo 只要保留一樣的欄位名稱即可
    @Bean
    @StepScope
    public FlatFileItemWriter<CLNS0B100ExportBodyVo> CLNS0B100ExportWriter(
            DbToFileContext jobContext) {
        jobContext.setIoFile(Paths.get(exportTempPath, "JCIC207_LONLXHLN01.txt"));
        String[] args = {"transCode", "financialCode", "space1", "applyDate", "customerId", "customerName", "engName", "birthday",
                "regAddr", "conZip", "conAddr", "conTelNum", "mobilePhoneNum", "space2", "degree", "conOwnCode", "servName",
                "servCorpId", "businessCode", "bussinessTelNum", "jobTitle", "servYear", "yearIncome", "dataDateYm", "sex",
                "citizenship", "passportNum", "oldTaxRegNum", "space3"};
       return new FlatFileItemWriterBuilder<CLNS0B100ExportBodyVo>()
                .name("CLNS0B100ExportWriter")
                .resource(new FileSystemResource(jobContext.getIoFile().toFile()))
                .shouldDeleteIfExists(true)
                .transactional(true)
                .encoding("MS950")
                .delimited()
                .delimiter("")
                .names(args)
                .build();
    }
}