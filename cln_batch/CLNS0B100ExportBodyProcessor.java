package cub.lip.bch.cln.processor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.chrono.MinguoDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import cub.lip.bch.cln.vo.CLNS0B100ExportBodyVo;

// TODO: 改成 ItemProcessor<CLNS0B100ExportBodyVo, CLNS0B100JCIC207ExportVo>（比照 PLNJ0B002CreateJCIC207Processor 的作法），
// process() 內部要 new 一個新的 CLNS0B100JCIC207ExportVo 把轉換結果 set 上去再回傳，
// 不要再直接 item.setXxx(...) 原地覆寫輸入的 item（現在 applyDate/birthday/customerId 等欄位都是覆寫輸入值，
// 讀取跟寫入用同一個 getter，容易搞混「這是原始值還是已格式化過的值」）
@StepScope
@Component
public class CLNS0B100ExportBodyProcessor implements ItemProcessor<CLNS0B100ExportBodyVo, CLNS0B100ExportBodyVo> {

    private static final Charset MS950 = Charset.forName("MS950");
    private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter ROC_YMD = DateTimeFormatter.ofPattern("yyyMMdd");
    private static final DateTimeFormatter ROC_YM = DateTimeFormatter.ofPattern("yyyMM");
    private static final Pattern CUSTOMER_ID_PATTERN = Pattern.compile("^[A-Za-z][12][0-9]{8}$");
    private static final BigDecimal TWELVE = new BigDecimal("12");
    private static final String[] VALID_DEGREES = { "1", "2", "3", "4", "5", "6" };

    /**
     * 將查詢出來的原始欄位轉換成固定長度的 JCIC207 匯出格式；customerId 格式不符則回傳 null（過濾掉該筆）
     * @param item
     * @return
     */
    @Override
    // TODO: 簽章要改成 public CLNS0B100JCIC207ExportVo process(CLNS0B100ExportBodyVo item)；
    // 下面每一行 item.setXxx(...) 都要改成 output.setXxx(...)，output 是新 new 出來的 CLNS0B100JCIC207ExportVo
    public CLNS0B100ExportBodyVo process(CLNS0B100ExportBodyVo item) {
        if (!isValidCustomerId(item.getCustomerId())) {
            return null;
        }
        item.setTransCode(fixed("A", 1, false));
        item.setFinancialCode(fixed("013", 3, false));
        item.setSpace1(fixed("", 4, false));
        item.setApplyDate(rocDate(item.getApplyDate(), 7));
        item.setCustomerId(fixed(item.getCustomerId(), 10, false));
        item.setCustomerName(fixed(item.getCustomerName(), 20, false));
        item.setEngName(fixed("", 20, false));
        item.setBirthday(rocDate(item.getBirthday(), 7));
        item.setRegAddr(fixed(joinAddr(item.getRegCityName(), item.getRegTownName(), item.getRegAddr()), 66, false));
        item.setConZip(fixed(item.getConZip(), 5, true));
        item.setConAddr(fixed(joinAddr(item.getConCityName(), item.getConTownName(), item.getConAddr()), 66, false));
        // TODO: 規格要求 conTelNum 應為 nightTelArea + "-" + nightTelNum + "#" + nightTelExt，
        // 需先確認 CLN_CUSTOMER_INFO 資料表這三個欄位的實際 DB 欄位名稱才能加到 Vo/SQL 裡
        item.setConTelNum(fixed(item.getHomePhone(), 16, false));
        item.setMobilePhoneNum(fixed(item.getCellPhone(), 16, false));
        item.setSpace2(fixed("", 5, false));
        item.setDegree(fixed(resolveDegree(item.getDegree()), 1, false));
        item.setConOwnCode(fixed(resolveConOwnCode(item.getConOwnCode()), 1, false));
        item.setServName(fixed(item.getServName(), 30, false));
        item.setServCorpId(fixed(item.getServCorpId(), 8, false));
        // TODO: 規格要求 businessCode 應透過 commCodeUtil.getAllComnCodes("BUSINESS_CODE_MAPPING_JCIC", ...) 查代碼表取得對應名稱，
        // 需先確認專案裡是否已有可注入的 commCodeUtil（或等價 Bean/類別路徑）
        item.setBusinessCode(fixed(item.getBusinessCode(), 6, false));
        // TODO: 規格要求 bussinessTelNum 應為 dayTelArea + "-" + dayTelNum + "#" + dayTelExt，
        // 目前誤用 servAddr（任職機構地址）；需先確認 CLN_CUSTOMER_INFO 這三個欄位的實際 DB 欄位名稱才能修正
        item.setBussinessTelNum(fixed(item.getServAddr(), 16, false));
        // TODO: 規格要求 jobTitle 應透過 commCodeUtil.getAllComnCodes("BUSINESS_CODE_MAPPING_JCIC2", ...) 查代碼表取得對應名稱，
        // 需先確認專案裡是否已有可注入的 commCodeUtil（或等價 Bean/類別路徑）
        item.setJobTitle(fixed(item.getJobTitle(), 10, false));
        item.setServYear(fixed(resolveServYear(item.getServMonth()), 2, true));
        item.setYearIncome(fixed(resolveYearIncome(item.getYearIncome()), 6, true));
        item.setDataDateYm(rocDate(item.getDataDate(), 5));
        item.setSex(fixed(resolveSex(item.getSex()), 1, false));
        // TODO: 規格要求依序檢查 citizenship1/citizenship2/citizenship3，取第一個非空白的值，
        // 需先確認來源資料表是否真的有這三個獨立欄位
        item.setCitizenship(fixed(item.getCitizenship(), 2, false));
        item.setPassportNum(fixed(item.getPassportNum(), 20, false));
        item.setOldTaxRegNum(fixed("", 10, false));
        item.setSpace3(fixed("", 36, false));
        return item;
    }

    /**
     * 檢查 customerId 是否符合「去除空白後10碼、第一碼英文、第二碼為1或2、其餘8碼數字」的格式
     * @param customerId
     * @return
     */
    private boolean isValidCustomerId(String customerId) {
        if (customerId == null) {
            return false;
        }
        return CUSTOMER_ID_PATTERN.matcher(customerId.trim()).matches();
    }

    /**
     * 教育程度白名單檢查，不在 1~6 名單內回傳空白
     * @param degree
     * @return
     */
    private String resolveDegree(String degree) {
        String trimmed = StringUtils.trim(degree);
        return ArrayUtils.contains(VALID_DEGREES, trimmed) ? trimmed : "";
    }

    /**
     * 自有住宅有無轉換：本人資產無/有貸款（1或2）轉 Y，其他轉 N
     * @param conOwnCode
     * @return
     */
    private String resolveConOwnCode(String conOwnCode) {
        String trimmed = StringUtils.trim(conOwnCode);
        return "1".equals(trimmed) || "2".equals(trimmed) ? "Y" : "N";
    }

    /**
     * 性別代碼轉換：2 轉 F，其他轉 M
     * @param sex
     * @return
     */
    private String resolveSex(String sex) {
        return "2".equals(StringUtils.trim(sex)) ? "F" : "M";
    }

    /**
     * 服務年資計算：無資料給空白，未滿12個月以1年計，滿12個月除以12無條件捨去到年
     * @param servMonth
     * @return
     */
    private String resolveServYear(String servMonth) {
        if (StringUtils.isBlank(servMonth)) {
            return " ";
        }
        BigDecimal months = new BigDecimal(servMonth.trim());
        if (months.compareTo(TWELVE) < 0) {
            return "1";
        }
        return months.divide(TWELVE, 0, RoundingMode.DOWN).toPlainString();
    }

    /**
     * 年收入轉換：乘以10四捨五入，結果為0時固定改為1
     * @param yearIncome
     * @return
     */
    private String resolveYearIncome(String yearIncome) {
        if (StringUtils.isBlank(yearIncome)) {
            return "";
        }
        BigDecimal amount = new BigDecimal(yearIncome.trim()).multiply(BigDecimal.TEN).setScale(0, RoundingMode.HALF_UP);
        if (amount.compareTo(BigDecimal.ZERO) == 0) {
            amount = BigDecimal.ONE;
        }
        return amount.toPlainString();
    }

    /**
     * 西元日期字串（yyyyMMdd）轉民國年格式；已是民國格式或長度不符 8 碼則原樣交給 fixed() 補位
     * @param value
     * @param length 7 為年月日，5 為年月
     * @return
     */
    private String rocDate(String value, int length) {
        if (StringUtils.isBlank(value)) {
            return fixed("", length, true);
        }
        String normalized = value.trim();
        if (normalized.length() == length) {
            return fixed(normalized, length, true);
        }
        if (normalized.length() != 8) {
            return fixed(normalized, length, true);
        }
        LocalDate date = LocalDate.parse(normalized, YYYYMMDD);
        MinguoDate minguoDate = MinguoDate.from(date);
        String roc = length == 5 ? minguoDate.format(ROC_YM) : minguoDate.format(ROC_YMD);
        return fixed(roc, length, true);
    }

    /**
     * 合併縣市、鄉鎮、地址成完整地址字串
     * @param city
     * @param town
     * @param addr
     * @return
     */
    private String joinAddr(String city, String town, String addr) {
        return defaultString(city) + defaultString(town) + defaultString(addr);
    }

    /**
     * 依 MS950 位元組長度裁切/補齊字串到固定長度
     * @param value
     * @param length
     * @param leftPadZero true 為右靠左補零，false 為左靠右補空白
     * @return
     */
    // TODO: 這個方法跟 ELCV0B002 專案的 cub.lip.bch.elc.util.BytePadUtil.truncateAndPadString(...) 功能重複，
    // 確認 cln_batch 這邊能否引用同一個共用 Util（或抽一份到共用 lib），改用共用方法取代這裡的土砲實作
    private String fixed(String value, int length, boolean leftPadZero) {
        String normalized = defaultString(value);
        byte[] bytes = normalized.getBytes(MS950);
        if (bytes.length > length) {
            normalized = trimToBytes(normalized, length);
        }
        bytes = normalized.getBytes(MS950);
        if (bytes.length == length) {
            return normalized;
        }
        String padChar = leftPadZero ? "0" : " ";
        StringBuilder builder = new StringBuilder();
        while ((builder.toString() + normalized).getBytes(MS950).length < length && leftPadZero) {
            builder.append(padChar);
        }
        while ((normalized + builder.toString()).getBytes(MS950).length < length && !leftPadZero) {
            builder.append(padChar);
        }
        return leftPadZero ? builder + normalized : normalized + builder;
    }

    /**
     * 依 MS950 位元組長度裁切字串到指定長度以內
     * @param value
     * @param length
     * @return
     */
    // TODO: BytePadUtil 內部應該也有等價的裁切邏輯，跟上面 fixed() 一起評估能否改用共用 Util
    private String trimToBytes(String value, int length) {
        String result = defaultString(value);
        while (result.getBytes(MS950).length > length && !result.isEmpty()) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    /**
     * null 轉為空字串
     * @param value
     * @return
     */
    // TODO: 跟 fixed()/trimToBytes() 一起評估能否改用共用 BytePadUtil；
    // 若只留這個 null 防呆，也可以直接用已 import 的 StringUtils.defaultString(value) 取代，不用自己寫
    private String defaultString(String value) {
        return value == null ? "" : value;
    }
}