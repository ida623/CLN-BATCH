package cub.lip.bch.cln.vo;

import lombok.Data;

// TODO: 拆成 input/output 兩個 Vo（比照 PLNJ0B002CustomerInfoVo / PLNJ0B002JCIC207UpdateVo 的作法）。
// 這個 Vo 之後只保留下面「查詢/輸入欄位」這段（第 36 行以前），
// 「輸出欄位」那段（transCode ~ space3）要搬到新檔案 CLNS0B100JCIC207ExportVo.java（同一個 cub.lip.bch.cln.vo package）。
@Data
public class CLNS0B100ExportBodyVo {

    private String customerId;
    private String customerName;
    private String engName;
    private String birthday;
    private String regAddr;
    private String regCityName;
    private String regTownName;
    private String conAddr;
    private String conCityName;
    private String conTownName;
    private String conZip;
    private String homePhone;
    private String cellPhone;
    private String servAddr;
    private String servName;
    private String servCorpId;
    private String businessCode;
    private String jobTitle;
    private String servMonth;
    private String yearIncome;
    private String degree;
    private String conOwnCode;
    private String sex;
    private String citizenship;
    private String passportNum;
    private String applyDate;
    private String dataDate;

    // TODO: 以下到檔案結尾的輸出欄位，要搬到新的 CLNS0B100JCIC207ExportVo，
    // 欄位順序照 CLNS0B100JobConfig.CLNS0B100ExportWriter() 裡 names[] 陣列的順序（29 個輸出欄位）
    /** 交易代碼 */
    private String transCode;
    private String financialCode;
    private String space1;
    private String conTelNum;
    private String mobilePhoneNum;
    private String space2;
    private String bussinessTelNum;
    private String servYear;
    private String dataDateYm;
    private String oldTaxRegNum;
    private String space3;
}
