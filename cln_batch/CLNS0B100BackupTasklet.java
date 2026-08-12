package cub.lip.bch.cln.tasklet;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import cub.lip.svc.batch.context.CommonContext;
import cub.lip.svc.batch.entity.BaseBatchDcl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@StepScope
public class CLNS0B100BackupTasklet implements Tasklet {

    @Autowired
    private CommonContext commonContext;

    @Value("${lip.export.ODS.path}")
    private String exportPath;

    @Value("${lip.export.temp.path}")
    private String exportTempPath;

    @Value("${lip.export.backup.path}")
    private String backupPath;

    @Value("${lip.export.flgInit.path}")
    private String flgInitPath;

    @Value("#{jobParameters[dataDate]}")
    private String dataDate;

    /**
     * 備份匯出檔案並搬移到正式輸出路徑，同時產生對應的 flg 檔
     * @param contribution
     * @param chunkContext
     * @return
     * @throws Exception
     */
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        BaseBatchDcl batchDcl = commonContext.getBatchDcl();
        String jobName = batchDcl != null ? batchDcl.getJobName() : "CLNS0_B100";
        String encode = StringUtils.defaultIfBlank(batchDcl != null ? batchDcl.getFileEncode() : null, "MS950");

        String ioFileName = "JCIC207_LONLXHLN01.txt";
        String flgName = "JCIC207_LONLXHLN01.flg";

        Path tempExpPath = Paths.get(exportTempPath, ioFileName);
        Path fileExpPath = Paths.get(exportPath, ioFileName);
        Path flagExpPath = Paths.get(flgInitPath, flgName);
        Path outFlagPath = Paths.get(exportPath, flgName);
        Path backupFile = Paths.get(backupPath, ioFileName);
        Path backupFlag = Paths.get(backupPath, flgName);

        if (Files.notExists(tempExpPath)) {
            throw new IllegalStateException("Export temp file not found: " + tempExpPath);
        }

        createDirectories(flagExpPath.getParent());
        createDirectories(fileExpPath.getParent());
        createDirectories(backupFile.getParent());
        createDirectories(backupFlag.getParent());

        Files.write(flagExpPath, ioFileName.getBytes(Charset.forName(encode)),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        Files.copy(tempExpPath, fileExpPath, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(flagExpPath, outFlagPath, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(tempExpPath, backupFile, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(flagExpPath, backupFlag, StandardCopyOption.REPLACE_EXISTING);

        safeDelete(tempExpPath);
        safeDelete(flagExpPath);

        log.info("[{}] export file and flag moved to output and backup paths", jobName);
        return RepeatStatus.FINISHED;
    }

    /**
     * 建立目錄，路徑為 null 或已存在則不動作
     * @param path
     * @throws IOException
     */
    private void createDirectories(Path path) throws IOException {
        if (path != null && Files.notExists(path)) {
            Files.createDirectories(path);
        }
    }

    /**
     * 刪除檔案，刪除失敗（例如檔案不存在）時忽略例外
     * @param path
     */
    private void safeDelete(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignore) {
        }
    }
}