package com.telcel.rcontrol.process.masivos;

import com.infomedia.context.APPContext;
import com.infomedia.database.DBException;
import com.infomedia.database.entity.dao.BaseDAO;
import com.infomedia.utils.DateUtils;
import com.infomedia.utils.DinamicVO;
import com.infomedia.utils.FileUtils;
import com.infomedia.utils.PropertyLoader;
import com.infomedia.utils.StringUtils;
import com.softcoatl.utils.file.Shell;
import com.telcel.rcontrol.ars.commons.TimestampParser;
import com.telcel.rcontrol.ars.model.RemedyEntryDB;
import com.telcel.rcontrol.ars.model.RemedyEntryVO;
import com.telcel.rcontrol.process.masivos.forms.ChangeMassive;
import com.telcel.rcontrol.process.masivos.forms.ChangeMassiveWorkLog;
import com.telcel.rcontrol.process.masivos.forms.SiteEPMSVVO;
import com.telcel.rcontrol.process.masivos.parser.ParserMasivosCIRelationship;
import com.telcel.rcontrol.services.remedy.generic.RemedyFault;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

import org.apache.log4j.Logger;

/**
 * <dl>
 * <dt><b>NMB:</b></dt><dd>MasivosCIRelationship</dd>
 * <dt><b>DSC:</b></dt><dd></dd>
 * </dl>
 *
 * @version 2.0
 * @author REV
 * @since 20/03/2012-18/06/2012
 */
public class MasivosCIRelationship extends MasivosCorCampo {

    private static final Logger LOG = Logger.getLogger(MasivosCIRelationship.class);
    

    public static final int CHG_STATUS = 7;
    public static final int CHG_SUBMITTER = 2;
    public static final int CHG_MODIFIED_DATE = 6;
    public static final int CHG_DESCRIPTION_ID = 1000000000;
    //public static final int Cambio_masivo = 536870951;

    public static final String ASSOCIATIONS_FORMNAME = "CHG:Associations";
    
    public static final String ATTR_RCONTROL_DOWNLOAD_PATH = "ars.remedy.download.path";
    public static final String RCONTROL_DOWNLOAD_PATH = "/home/remedy/download/MASIVOS";
    public static final String ATTR_MASIVOS_COR_ADJUNTO = "masivos.ci.relationship.filename";
    public static final String ATTR_MASIVOS_COR_ERR_ATTACHMENT = "masivos.ci.relationship.err.attachment";
    public static final String ATTR_MASIVOS_COR_ERR_PARSER = "masivos.ci.relationship.err.parser";
    public static final String PRPT_TT_COR_TITLE_PREFIX = "masivos.ci.relationship.tt.title.prefix";
    public static final String PRPT_TITLE = "RELACION MASIVA DE CIS";

    protected List<RemedyEntryDB> goCRList = new ArrayList<>();
    protected Calendar goExecutionTime = null;
    
    public String companyCRQ;
    
    public static final String CR_GROUP_CENAM [] = {"GTM-WO ING CORE REG", "GTM-WO ING SVA", "GTM-WO OPTIMIZACION REGIONAL","GTM-WO ING RED", 
                                                "GTM-WO-ING OPTIMIZACION", "GTM-WO ING ACCESO IP_TX","GTM-WO ING HOMOLOGACION", "GTM-WO CORE IP-TX",
                                                "GTM-WO ING MOVIL IMPLEMENTACION", "GTM-WO IMPLEMENTACION PLANTA INTERNA","GTM-OYM CCR","CRI-WO ING CORE", 
                                                "CRI-WO ING TRANSMISION IP", "CRI-WO ING RF","CRI-WO ING IMPLEMENTACION","SLV-TECNICA INGENIERIA IP", 
                                                "SLV-TECNICA INGENIERIA CONMUTACION Y SVA", "SLV-TECNICA INGENIERIA CELULAR","SLV-TECNICA INGENIERIA ACCESO",
                                                "HND-JEFATURA DE OPTIMIZACION RADIOFRECUENCIA","HND-JEFATURA INGENIERIA CORE MOVIL","HND-JEFATURA INGENIERIA IP",
                                                "HND-JEFATURA TRANSMISION MICROONDAS","HND-SUBGERENCIA DE INGENIERIA DE TRANSPORTE",
                                                "HND-SUBGERENCIA DE IMPLANTACION Y CONSTRUCCION","NIC-ING CORE","NIC-RF MOVIL","NIC-ING DX TX","NIC-INGENIERIA CORE",
                                                "NIC-INGENIERIA DATOS"};//Puede relacionar sitios de todo CENAM
    public static final String CR_GROUP_CENAM2 [] = {"GTM-OYM CCR","CRI-OYM CCR", "HND-OYM CCR", "SLV-OYM CCR", "NIC-OYM CCR"};//relaciona sitios de GTM + su propio TENAM
    
//    public static final String CR_GROUP_GTM [] = {"GTM-WO ING RED", "GTM-WO-ING OPTIMIZACION", "GTM-WO ING ACCESO IP_TX",
//                                                "GTM-WO ING HOMOLOGACION", "GTM-WO CORE IP-TX","GTM-WO ING MOVIL IMPLEMENTACION",
//                                                "GTM-WO IMPLEMENTACION PLANTA INTERNA","GTM-OYM CCR"};
//    public static final String CR_GROUP_CRI [] = {"CRI-WO ING CORE", "CRI-WO ING TRANSMISION IP", "CRI-WO ING RF","CRI-WO ING IMPLEMENTACION"};
//    public static final String CR_GROUP_SLV [] = {"SLV-TECNICA INGENIERIA IP", "SLV-TECNICA INGENIERIA CONMUTACION Y SVA",
//                                                "SLV-TECNICA INGENIERIA CELULAR","SLV-TECNICA INGENIERIA ACCESO" };
//    public static final String CR_GROUP_HND [] = {"HND-JEFATURA DE OPTIMIZACION RADIOFRECUENCIA","HND-JEFATURA INGENIERIA CORE MOVIL","HND-JEFATURA INGENIERIA IP",
//                                                "HND-JEFATURA TRANSMISION MICROONDAS","HND-SUBGERENCIA DE INGENIERIA DE TRANSPORTE",
//                                                "HND-SUBGERENCIA DE IMPLANTACION Y CONSTRUCCION"};
//    public static final String CR_GROUP_NIC [] = {"NIC-ING CORE","NIC-RF MOVIL","NIC-ING DX TX","NIC-INGENIERIA CORE","NIC-INGENIERIA DATOS"};
    
    public static final String CR_COMPANY_CENAM ="CENAM";
    
    /**
     * <dl>
     * <dt><b>NMB:</b></dt><dd>MasivosCIRelationship</dd>
     * <dt><b>DSC:</b></dt><dd>Constructor</dd>
     * </dl>
     *
     * @author REV
     * @throws Exception If there was an error
     * @since 20/03/2012-20/03/2012
     */
    public MasivosCIRelationship() throws Exception {
        super();
    }//MasivosCIRelationship

@Override
    protected void retrieve() throws Exception {
        TimestampParser tsp = new TimestampParser();
        String vsConditionalString = "";
        LOG.info("Retrieving since " + goExecutionTime.getTime());
        vsConditionalString += "'" + CHG_STATUS + "'=8";
        vsConditionalString += " AND '" + CHG_MODIFIED_DATE + "'>" + tsp.parseTimeStamp(goExecutionTime);
        vsConditionalString += " AND '" + CHG_DESCRIPTION_ID + "'=\"" + PRPT_TITLE + "\"";
        //vsConditionalString += "'" + Cambio_masivo + "'=\"SI\"";        //////////////CAMPO BANDERA "SI"

        LOG.info("Retrieving from " + ChangeMassive.FORM_NAME + ":" + vsConditionalString);

        for (RemedyEntryDB entry : R_Client.retrieveEntries(ChangeMassive.FORM_NAME,
                vsConditionalString,
                new int[]{1, 7, 8,  1000000151, 1000000350, 1000000362, 1000000000, 1000003230, 200000012, 
                        1000001270, 1000001271, 1000001272, 1000000063, 1000000064, 1000000065, 1000000182, 1000003229,
                        1000000082, 1000000001})) {
            if (PRPT_TITLE.equals(entry.NVL(1000000000))) { //verificar resumen del CRQ
                    goCRList.add(entry);
            }
        }
    }

    protected boolean isValidAttachment(String fileName) {
        LOG.debug(fileName + " vs " + getProperty(ATTR_MASIVOS_COR_ADJUNTO));
        return fileName.contains(getProperty(ATTR_MASIVOS_COR_ADJUNTO));
    }

    protected List<DinamicVO> parseAttachment(ChangeMassive poCR) throws Exception {
        ParserMasivosCIRelationship voParser = new ParserMasivosCIRelationship();
        String fileName = "";
        String wlID = null;
        int fieldID = 0;
        voParser.setWorkingDir(RCONTROL_DOWNLOAD_PATH);

        try {
            LOG.info("Retrieve Attachments from " + poCR.NVL(ChangeMassive.CR_INF_CHG_ID));
            for (RemedyEntryDB entry : R_Client.retrieveEntries(ChangeMassiveWorkLog.FORM_NAME, "'1000000182'=\"" + poCR.NVL(ChangeMassive.CR_INF_CHG_ID) + "\"", new int[]{1, ChangeMassiveWorkLog.WL_ATTACHMENT_1, ChangeMassiveWorkLog.WL_ATTACHMENT_2, ChangeMassiveWorkLog.WL_ATTACHMENT_3})) {
                if (isValidAttachment(entry.NVL(ChangeMassiveWorkLog.WL_ATTACHMENT_1))) {
                    wlID = entry.getARSCoreID();
                    fileName = entry.NVL(ChangeMassiveWorkLog.WL_ATTACHMENT_1);
                    fieldID = ChangeMassiveWorkLog.WL_ATTACHMENT_1;
                    break;
                } else if (isValidAttachment(entry.NVL(ChangeMassiveWorkLog.WL_ATTACHMENT_2))) {
                    wlID = entry.getARSCoreID();
                    fileName = entry.NVL(ChangeMassiveWorkLog.WL_ATTACHMENT_2);
                    fieldID = ChangeMassiveWorkLog.WL_ATTACHMENT_2;
                    break;
                } else if (isValidAttachment(entry.NVL(ChangeMassiveWorkLog.WL_ATTACHMENT_3))) {
                    wlID = entry.getARSCoreID();
                    fileName = entry.NVL(ChangeMassiveWorkLog.WL_ATTACHMENT_3);
                    fieldID = ChangeMassiveWorkLog.WL_ATTACHMENT_3;
                    break;
                }
            }
            if (!StringUtils.isNVL(wlID)) {
                LOG.info("Dowloading: " + fileName);
                if (R_Client.download(wlID, fieldID)) {
                    LOG.info("Parsing " + fileName);
                    LOG.info("get attachment dir "+voParser.getAttachmentDir());
                    voParser.parse(fileName);
                    LOG.info("get items "+voParser.getCRItems());
                }
            } else {
                throw new Exception(getProperty(ATTR_MASIVOS_COR_ERR_PARSER).replaceAll("<CRID>", poCR.getARSCoreID()));
            }
        } catch (Exception voEXC) {
            throw voEXC;
        } finally {
            try {
                FileUtils.prcEliminaArchivo(APPContext.getInitParameter(ATTR_RCONTROL_DOWNLOAD_PATH), fileName);
            } catch (Exception EXC) {
                LOG.error("ERROR PARSE: "+EXC);
            }
        }

        return voParser.getCRItems();
    }//parseAttachment

    protected void processVOList(ChangeMassive poCR, List<DinamicVO> poVOList) {
        SiteEPMSVVO voSite = null;

        for (DinamicVO voVO : poVOList) {
            LOG.info("Parsed VO:" + voVO.toString() + "\n\n");
            try {
                voSite = getSite(voVO.getCampo(ParserMasivosCIRelationship.CR_SITE_ID), voVO.getCampo(ParserMasivosCIRelationship.CR_COMPANY));
                
                //if (validateCompany(poCR.NVL(1000003229), ParserMasivosCIRelationship.CR_COMPANY)){
                //1000003229=grupo asignado (coord.) del cambio; 1000000001=location company del cambio
                LOG.info("Compañia del CRQ:... " + poCR.NVL(1000000082));
                LOG.info("DATOS RESTANTES"+poCR.NVL(1000003229)+voSite);
                
                if (validateCompany(poCR.NVL(1000003229), poCR.NVL(1000000082), voSite)){
                    relateSite(poCR, voSite);
                }
                else
                    LOG.info("El sitio " + voSite.getValue(SiteEPMSVVO.SITE_ID_EP) + " no pertenece a la compañia del grupo creador del CRQ");
            } catch (Exception voEXC) {
                addErrors(voVO.getCampo(ParserMasivosCIRelationship.CR_SITE_ID), null != voSite ? voSite.NVL(SiteEPMSVVO.REAL_STATUS_EP) : "", voEXC);
            }
        }
    }
    
    public boolean validateCompany(String asigned_groupCRQ, String companyCRQ, SiteEPMSVVO voSite) {
    //    boolean company = true;
    //    int index = 0;
        
        String siteID      = voSite.getValue(SiteEPMSVVO.SITE_ID_EP);
        String siteCompany = voSite.getValue(SiteEPMSVVO.SITE_COMPANY_EP);
        LOG.info("**Validando el sitio..." + siteID
                            + " compañia: "+ siteCompany);
        LOG.info("Grupo asignado del CRQ: " + asigned_groupCRQ);
        LOG.info("Compañia del CRQ: " + companyCRQ);
        this.companyCRQ = companyCRQ;
        
        switch (siteCompany){
    //    switch (companyCRQ){
//            case "CLARO GUATEMALA":
//                LOG.info("EL NODO ES DE: GTM '" + siteCompany + "'... C=0");
//                if( retrieveGRP(asigned_groupCRQ, CR_GROUP_GTM, siteCompany, 0)){
//                    return true;
//                }else{
//                    LOG.info("EL NODO ES DE: GTM CENAM '" + siteCompany + "'... C=5");
//                    return retrieveGRP(asigned_groupCRQ, CR_GROUP_CENAM, siteCompany, 5);
//                }
//            case "CLARO COSTA RICA":
//                LOG.info("EL NODO ES DE: CRI '" + siteCompany + "'... C=1");
//                if(retrieveGRP(asigned_groupCRQ, CR_GROUP_CRI, siteCompany, 1)){
//                    return true;
//                }else{
//                    LOG.info("EL NODO ES DE: CRI CENAM '" + siteCompany + "'... C=5");
//                    return retrieveGRP(asigned_groupCRQ, CR_GROUP_CENAM, siteCompany, 5);
//                }
//            case "CLARO EL SALVADOR":
//                LOG.info("EL NODO ES DE: SLV '" + siteCompany + "'... C=2");
//                if(retrieveGRP(asigned_groupCRQ, CR_GROUP_SLV, siteCompany, 2)){
//                    return true;
//                }else{
//                    LOG.info("EL NODO ES DE: SLV CENAM '" + siteCompany + "'... C=5");
//                    return retrieveGRP(asigned_groupCRQ, CR_GROUP_CENAM, siteCompany, 5);
//                }
//            case "CLARO HONDURAS":
//                LOG.info("EL NODO ES DE: HND '" + siteCompany + "'... C=3");
//                if(retrieveGRP(asigned_groupCRQ, CR_GROUP_HND, siteCompany, 3)){
//                    return true;
//                }else{
//                    LOG.info("EL NODO ES DE: HND CENAM '" + siteCompany + "'... C=5");
//                    return retrieveGRP(asigned_groupCRQ, CR_GROUP_CENAM, siteCompany, 5);
//                }
//            case "CLARO NICARAGUA":
//                LOG.info("EL NODO ES DE: NIC '" + siteCompany + "'... C=4");
//                if(retrieveGRP(asigned_groupCRQ, CR_GROUP_NIC, siteCompany, 4)){
//                    return true;
//                }else{
//                    LOG.info("EL NODO ES DE: NIC CENAM '" + siteCompany + "'... C=5");
//                    return retrieveGRP(asigned_groupCRQ, CR_GROUP_CENAM, siteCompany, 5);
//                }
            case "CENAM":
                LOG.info("EL NODO ES DE: CENAM '" + siteCompany + "'... C=5");
                return retrieveGRP(asigned_groupCRQ, CR_GROUP_CENAM, siteCompany, 5);

            default:
                LOG.info("EL CRQ ES DEL GRUPO: '" + asigned_groupCRQ + "'... NO SE RELACIONA");
                LOG.info("EL NODO ES DE COMP: '" + siteCompany + "'... NO SE RELACIONA");
                return false;
        }
/*        
        if (siteCompany.equals("TELCEL")){
                company = false;
                LOG.info("EL NODO ES DE TELCEL");
            }
    */    
    //    return company;
    }
    
    public boolean retrieveGRP(String asigned_groupCRQ, String [] groupList/*, String [] Company*/,String siteCompany, int C){
        boolean company = false;
        LOG.info("HACIENDO retrieveGRP: ASSG GRP: " + asigned_groupCRQ + ", NOMBRE GRP: "+ groupList.toString() + ", COMP: " +  siteCompany + ", C="+C);
                            //NIC ING CORE       //CR_GROUP_HND(HND-JEFATURA, HND-SUBGERE)       //CLARO HONDURAS      //3
                            //NIC ING CORE       //CR_GROUP_NIC(NIC ING CORE, NIC-RF MOVIL)      //CLARO NICARAGUA     //3
                                            
                            //GTM-WO ING CORE REG   //CR_GROUP_CENAM(GTM-WO ING CORE REG,GTM-WO ING SVA)    //CLARO NICARAGUA      //5 ---
                            //GTM-OYM CCR           //CR_GROUP_CENAM2(GTM-OYM CCR","CRI-OYM CCR)            //CLARO GUATEMALA      //5
        LOG.info("HACIENDO retrieveGRP: COMP CRQ: " + this.companyCRQ);
        if(CR_COMPANY_CENAM.equals(siteCompany)){
                    LOG.info("retrieveGRP: EL NODO ES DE " + CR_COMPANY_CENAM + "... CORRECTO");
                    company = true;
        }else{
                    LOG.info("retrieveGRP: EL NODO ES DE " + CR_COMPANY_CENAM + "... CORRECTO");
                    company = false;
        }
//        
//        for (String grp : groupList){
//            if (grp.equals(asigned_groupCRQ)){      /******************************************************/
//                LOG.info("retrieveGRP: EL CRQ ES DEL GRUPO " + grp + "... CORRECTO 1");
//                
//                if(CR_COMPANY_CENAM.equals(siteCompany)){
//                    LOG.info("retrieveGRP: EL NODO ES DE " + CR_COMPANY_CENAM + "... CORRECTO 1");
//                    company = true;
//                }
//                else if (C==5){
//                    LOG.info("C=5");/*
//                    for (String comp : CR_COMPANY_CENAM){
//                        if(comp.equals(siteCompany)){
//                            LOG.info("retrieveGRP: EL NODO ES DE " + comp + "... CORRECTO 1*5");
//                            company = true;
//                        }/*else{
//                            LOG.info("retrieveGRP: EL NODO ES DE " + comp + "... INCORRECTO 1*5");
//                            company = false;
//                 //       }
//                    }*/
//                    company = true;        
//                }else{
//                    LOG.info("retrieveGRP: EL NODO ES DE '" + siteCompany + "'... INCORRECTO*");
//                    company = false;
//                }
//            }else {              /******************************************************/
//                for (String grp_2 :  CR_GROUP_CENAM2){
//                    if (grp_2.equals(asigned_groupCRQ)){
//                        LOG.info("retrieveGRP: EL CRQ ES DEL GRUPO: " + grp_2 + "... EXISTE EN CENAM2, CORRECTO 2");
//                /*        return true;
//                    }
//                    else{*/
//                        if(siteCompany.equals(this.companyCRQ /*CR_COMPANY_CENAM[C]*/) || siteCompany.equals("CLARO GUATEMALA")){
//                            LOG.info("retrieveGRP: EL NODO ES DE: " + siteCompany + "... CORRECTO 2");
//                            company = true;
//                        }
//                        else{
//                            LOG.info("retrieveGRP: EL NODO ES DE: " + siteCompany + "... INCORRECTO 2");
//                            company = false;
//                        }
//                    }
//                //    }
//                }
//            }
//        }
        
    /*    
        for (String grp :  CR_GROUP_CENAM){
            if (grp.equals(asigned_group)){
                
                for (String comp : CR_COMPANY_CENAM){
                    if (comp.equals(siteCompany)){
                        company = true;
                    }
                }
                
            }
        }
        */
        
        return company;
    }
    
    

    protected void relateSite(ChangeMassive voCRQ, SiteEPMSVVO voSite) {
        boolean vsAssoc = false;
        String vsConditionalString = "'1000000206' = \"" + voSite.getValue(SiteEPMSVVO.SITE_ID_EP) + "\""
                + " AND '1000000203' = \"CHG:Infrastructure Change\""
                + " AND '1000000205' = \"" + voCRQ.NVL(1000000182) + "\"";
        try {
            //deleteGenericAssociation(voCRQ, voSite);
            if (voSite.isOperando()) {
                LOG.info("Relacionando el sitio..." + voSite.getValue(SiteEPMSVVO.SITE_ID_EP));
                RemedyEntryDB assEntry = new RemedyEntryDB();
                //Entrada 1
                assEntry.setValue(1000000101, voSite.getValue(SiteEPMSVVO.SITE_FORMNAME));
                assEntry.setValue(1000000204, voSite.getValue(SiteEPMSVVO.SITE_RECONID));
                assEntry.setValue(1000000211, "6000");
                assEntry.setValue(1000000206, voSite.getValue(SiteEPMSVVO.SITE_ID_EP));
                assEntry.setValue(230000009, voSite.NVL(536878270));
                //Entrada 2
                assEntry.setValue(1000000203, "CHG:Infrastructure Change");
                assEntry.setValue(1000000205, voCRQ.NVL(1000000182));
                //Otros
                assEntry.setValue(1000000208, "35000");
                assEntry.setValue(301569500, "40000");
                assEntry.setValue(1000000216, voCRQ.NVL(1000000182));
                assEntry.setValue(1000002706, voSite.getValue(SiteEPMSVVO.SITE_ID_EP));
                
        //        assEntry.setValue(536870951, "SI");     ////agregar campo "bandera"

                R_Client.lookANDcreate(ASSOCIATIONS_FORMNAME, assEntry, vsConditionalString);

            } else {
                LOG.error("El sitio "+voSite+" no esta OPERANDO");
            }
        } catch (RemedyFault ex) {
            LOG.error("ERROR relateSite: " + ex);
            ex.printStackTrace();
        }
    }

    protected boolean lookAssociation(ChangeMassive voCRQ, SiteEPMSVVO voSite) {
        String vsConditionalString = "";
        boolean vsAssoc = false;
        vsConditionalString += "'1000000206' = \"" + voSite.getValue(SiteEPMSVVO.SITE_ID_EP) + "\"";
        vsConditionalString += " AND '1000000203' = \"CHG:Infrastructure Change\"";
        vsConditionalString += " AND '1000000205' = \"" + voCRQ.NVL(1000000182) + "\"";

        LOG.info("Retrieving from " + ASSOCIATIONS_FORMNAME + ":" + vsConditionalString);

        try {
            RemedyEntryDB entry = R_Client.simpleRetrieve(
                    ASSOCIATIONS_FORMNAME, 
                    vsConditionalString, 
                    new int[]{1});
            if (entry != null){
                LOG.   info("Entrada encontrada: " + entry.isNVL(1) + " con valor " +entry.NVL(1));
                if (entry.isNVL(1)){
                    vsAssoc = true;
                }else{
                    LOG.info("El sitio " + voSite.getValue(SiteEPMSVVO.SITE_ID_EP) + " ya ha sido relacionado al cambio");
                }
            }
        } catch (Exception ex) {
            LOG.error("ERROR lookAssociation: " + ex);
        }
        return vsAssoc;
    }
    
    protected boolean deleteGenericAssociation(ChangeMassive voCRQ, SiteEPMSVVO voSite){
        String vsConditionalString = "";
        String vsAssoc = "";
        vsConditionalString += "'1000000206' = \"MASIVO_GENERICO\"";
        vsConditionalString += " AND '1000000203' = \"CHG:Infrastructure Change\"";
        vsConditionalString += " AND '1000000205' = \"" + voCRQ.NVL(1000000182) + "\"";

        LOG.info("Retrieving from " + ASSOCIATIONS_FORMNAME + ":" + vsConditionalString);

        try {
            for (RemedyEntryDB entry : R_Client.retrieveEntries(ASSOCIATIONS_FORMNAME, vsConditionalString, new int[]{1})) {
                LOG.info("Entrada encontrada: " + entry.isNVL(1) + " con valor " +entry.NVL(1));
                if (entry.isNVL(1)){
                    R_Client.update("CHG:Associations", entry.NVL(1), "'1000000205'='CRQPROCESADO' '1000000216'='CRQPROCESADO'");
                }else{
                    LOG.info("El sitio " + voSite.getValue(SiteEPMSVVO.SITE_ID_EP) + " ya ha sido relacionado al cambio");
                }
            }
        } catch (Exception ex) {
            LOG.error("ERROR deleteGeneric: " + ex);
        }
        return true;        
    }
    
    private String readLocalHost() {
        Shell shell;
        String vsLocalhost = "";
        try {
            shell = new Shell();
            shell.exec("hostname");
            vsLocalhost = shell.result();
            if (!StringUtils.isNVL(shell.error())) {
                LOG.error("ERROR readLocalHost - SHELL: "+shell.error());
            }
        } catch (Exception voEXC) {
            LOG.error("ERROR readLocalHost - :"+voEXC);
        }

        return vsLocalhost.toUpperCase();
    }//getLocalHost

    private boolean lock(ChangeMassive cr) {
        StringBuilder lock = new StringBuilder();
        lock.append("INSERT INTO MSV_COR_CRS (CRQ, CRQ_HOST) ");
        lock.append("VALUES ('").append(cr.NVL(ChangeMassive.CR_INF_CHG_ID)).append("', '").append(readLocalHost()).append("')");
        try {
            return BaseDAO.execute("RControl", lock.toString());
        } catch (DBException DBE) {
            LOG.error("ERROR LOCK: " + DBE);
        }
        return false;
    }//lock

    @Override
    protected void process() {
        ChangeMassive voCR = null;

        try {
            if (goCRList.isEmpty()) {
                LOG.info("No CRQ to process!");
            } else {
                for (RemedyEntryVO voCRVO : goCRList) {
                    try {
                        voCR = new ChangeMassive(voCRVO);
                        if (lock(voCR)) {
                            processVOList(voCR, parseAttachment(voCR));
                            //voCRVO.NVL(1000003229);
                        }
                    } catch (Exception EXC) {
                        LOG.error("ERROR PROCESS CRQ: "+ EXC);
                    } finally {
                        //LOG.info("¿Tiene hijos? " + voCR.hasChildren());
                        //if (null != voCR && voCR.hasChildren()) {}
                        LOG.info("Avanza el CRQ con ID: " + voCR.NVL(1000000182));
                        goToImplementationInProgress(voCR.NVL(1000000182));
                        addProcessed(voCR);
                    }
                }//foreach CR
            }
        } catch (Exception EXC) {
            LOG.error("ERROR PROCESS 2: "+EXC);
        }
    }//process

    @Override
    public void execute() {
        if (goExecutionTime == null) {
            goExecutionTime = DateUtils.fncoTrucado(Calendar.getInstance(), Calendar.DAY_OF_YEAR);
        }
        super.execute();
    }

    @Override
    public void execute(Calendar poExecutionTime) {
        goExecutionTime = poExecutionTime;
        execute();
    }

    @Override
    public void execute(String psExecutionTime) {
        execute(StringUtils.isNVL(psExecutionTime) ? null : DateUtils.fncoCalendar(psExecutionTime));
    }

    @Override
    public String getSubject() {
        return "REPORTE DE RELACIONES MASIVAS DE CIS";
    }

    @Override
    protected void results() {
        //Nothing here
    }//results
    
    public static void main(String[] psParametros) {
        MassiveCreationProcess voProceso;
        Properties voProperties;
        try {
            voProperties = PropertyLoader.load("masivos.properties");
            MassiveContext.getInstance().configure(PropertyLoader.load("rcontrol.properties"));
            MassiveContext.getInstance().configure(voProperties);
            MassiveContext.getInstance().initDataBaseService();
            MassiveContext.getInstance().initMailService();
            voProceso = new MasivosCIRelationship();
            voProceso.execute(psParametros.length > 0 ? psParametros[0] : "");
        } catch (Exception EXC) {
            Logger.getLogger(MasivosCorCampo.class).fatal("FATAL MasivosCIRelationship...", EXC);
        } finally {
            MassiveContext.getInstance().destroyContext();
        }
    }//main
}//MasivosCIRelationship
