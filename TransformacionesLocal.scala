package Recogida.Common

import Recogida.spark
import org.apache.spark.sql.functions.{col, lit, when, max, to_timestamp}
import org.apache.spark.storage.StorageLevel
import spark.implicits._

import java.sql.Timestamp
import java.util.Date

object TransformacionesLocal extends {

  def RecogidaInicial(date: String): Unit = {

    /** Solo cargamos los que se usan en la consulta */
      /** Quitamos las columnas que no se usan */

    val SGE_SAP = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_SGE_SAP_PROVEEDORES_20220624_165906.csv.bz2").select("PROVE_NM", "PROVE_ID")


    /**
    val SGE_SAP_FILTERED = SGE_SAP.filter(
      (SGE_SAP("EXENTO_DESDE_DT").lt(lit("2017-07-31")) && SGE_SAP("EXENTO_DESDE_DT").gt(lit("2017-07-01")))
        || (SGE_SAP("EXENTO_DESDE_DT").lt(lit("2017-07-01")) && (SGE_SAP("EXENTO_HASTA_DT").gt(lit("2017-07-01"))  ||  SGE_SAP("EXENTO_HASTA_DT").isNull))
    )
    */

    val SGR_MU_UF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_SGR_MU_ASIG_OPERADORES_UF_TMP_20220624_165920.csv.bz2")

    val SGR_MU_UF_FILTERED = SGR_MU_UF.filter(
      (SGR_MU_UF("DESDE_DT").lt(lit("2017-08-01")) && SGR_MU_UF("DESDE_DT").gt(lit("2017-06-30")))
        || (SGR_MU_UF("DESDE_DT").lt(lit("2017-07-01")) && (SGR_MU_UF("HASTA_DT").gt(lit("2017-07-01"))  ||  SGR_MU_UF("HASTA_DT").isNull))
    )


    /** Este no se filtra (no tiene fechas) */
    val SGR_MU_UTE = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_SGR_MU_ASIG_OPERADORES_UTE_TMP_20220624_165929.csv.bz2")



    /** Este no se filtra (no tiene fechas) */
    val VM_COMUAUTO = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_COMUAUTO_20220624_171044.csv.bz2")
      .select("COMAU_ID")

    /**
    val VM_ELTREMED = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_ELTREMED_20220624_172354.csv.bz2")
    */

    val VM_ELTREPOB = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_ELTREPOB_20220624_172746.csv.bz2")

    val VM_ELTREPOB_FILTERED = VM_ELTREPOB.filter(
      (VM_ELTREPOB("DESDE_DT").lt(lit("2017-08-01")) && VM_ELTREPOB("DESDE_DT").gt(lit("2017-06-30")))
        || (VM_ELTREPOB("DESDE_DT").lt(lit("2017-07-01")) && (VM_ELTREPOB("HASTA_DT").gt(lit("2017-07-01"))  ||  VM_ELTREPOB("HASTA_DT").isNull))
    ).select("DESDE_DT","POBLA_QT","POBIN_QT","UFTRG_ID","VERSI_ID", "HASTA_DT")

    /** Sin filtro (no hay fechas) */
    val VM_ENTLOCAL = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_ENTLOCAL_20220624_171535.csv.bz2")
      .select("ELMUN_ID")

    val VM_ENTLTPRE = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_ENTLTPRE_20220624_172757.csv.bz2")

    val VM_ENTLTPRE_FILTERED = VM_ENTLTPRE.filter(
      (VM_ENTLTPRE("DESDE_DT").lt(lit("2017-08-01")) && VM_ENTLTPRE("DESDE_DT").gt(lit("2017-06-30")))
        || (VM_ENTLTPRE("DESDE_DT").lt(lit("2017-07-01")) && (VM_ENTLTPRE("HASTA_DT").gt(lit("2017-07-01"))  ||  VM_ENTLTPRE("HASTA_DT").isNull))
    )

    /**
    val VM_MEDPERST = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_MEDPERST_20220624_171055.csv.bz2")
    */

      /** En la consulta no lo filtra */

    val VM_POBPERST = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_POBPERST_20220624_172950.csv.bz2")
      .select("UFUGA_ID", "DESDE_DT")


    val VM_TIPOLENT = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_TIPOLENT_20220624_174014.csv.bz2")

    val VM_TIPOLENT_FILTERED = VM_TIPOLENT.filter(
      (VM_TIPOLENT("DESDE_DT").lt(lit("2017-08-01")) && VM_TIPOLENT("DESDE_DT").gt(lit("2017-06-30")))
        || (VM_TIPOLENT("DESDE_DT").lt(lit("2017-07-01")) && (VM_TIPOLENT("HASTA_DT").gt(lit("2017-07-01"))  ||  VM_TIPOLENT("HASTA_DT").isNull))
    ).select("ELMUN_ID", "TPENT_ID")

    val VM_TIPOLFAC = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_TIPOLFAC_20220624_174344.csv.bz2")

    //JOIN CON TIPOLFAC FILTERED EN VEZ DE TIPOLFAC
    val VM_TIPOLFAC_FILTERED = VM_TIPOLFAC.filter(
      (VM_TIPOLFAC("DESDE_DT").lt(lit("2017-08-01")) && VM_TIPOLFAC("DESDE_DT").gt(lit("2017-06-30")))
        || (VM_TIPOLFAC("DESDE_DT").lt(lit("2017-07-01")) && (VM_TIPOLFAC("HASTA_DT").gt(lit("2017-07-01"))  ||  VM_TIPOLFAC("HASTA_DT").isNull))
    ).select("TPGFA_ID", "ELMUN_ID")

    val VM_TPRECOGI = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_TPRECOGI_20220624_174341.csv.bz2")
      .select("PROCE_ID", "TPREC_ID")

    val VM_UAACTIVI = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_UAACTIVI_20220624_174209.csv.bz2")

    val VM_UAACTIVI_FILTERED = VM_UAACTIVI.filter(
    (VM_UAACTIVI("DESDE_DT").lt(lit("2017-08-01")) && VM_UAACTIVI("DESDE_DT").gt(lit("2017-06-30")))
    || (VM_UAACTIVI("DESDE_DT").lt(lit("2017-07-01")) && (VM_UAACTIVI("HASTA_DT").gt(lit("2017-07-01"))  ||  VM_UAACTIVI("HASTA_DT").isNull))
      && ((VM_UAACTIVI("ACTIV_ID") === 1) || (VM_UAACTIVI("ACTIV_ID") === 2))) // Condición del where nos la quitamos aquí
      .select("UNADM_ID", "UAACT_ID", "ACTIV_ID")

    val VM_UFTRGMUN = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_UFTRGMUN_20220624_174447.csv.bz2")

    val VM_UFTRGMUN_FILTERED = VM_UFTRGMUN.filter(
      (VM_UFTRGMUN("DESDE_DT").lt(lit("2017-08-01")) && VM_UFTRGMUN("DESDE_DT").gt(lit("2017-06-30")))
        || (VM_UFTRGMUN("DESDE_DT").lt(lit("2017-07-01")) && (VM_UFTRGMUN("HASTA_DT").gt(lit("2017-07-01"))  ||  VM_UFTRGMUN("HASTA_DT").isNull))
    ).select("UFUGA_ID", "MUNTR_ID", "UFTRG_ID")

    val VM_UFUGACTI = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_UFUGACTI_20220624_174339.csv.bz2")


    val VM_UFUGACTI_FILTERED = VM_UFUGACTI.filter(
      (VM_UFUGACTI("DESDE_DT").lt(lit("2017-08-01")) && VM_UFUGACTI("DESDE_DT").gt(lit("2017-06-30")))
        || (VM_UFUGACTI("DESDE_DT").lt(lit("2017-07-01")) && (VM_UFUGACTI("HASTA_DT").gt(lit("2017-07-01"))  ||  VM_UFUGACTI("HASTA_DT").isNull))
    ).select("UGACT_ID", "DESDE_DT", "UNFAC_ID", "UFUGA_ID", "HASTA_DT")


    val VM_UGACTIVI = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_UGACTIVI_20220624_174307.csv.bz2")

    val VM_UGACTIVI_FILTERED = VM_UGACTIVI.filter(
      (VM_UGACTIVI("DESDE_DT").lt(lit("2017-08-01")) && VM_UGACTIVI("DESDE_DT").gt(lit("2017-06-30")))
        || (VM_UGACTIVI("DESDE_DT").lt(lit("2017-07-01")) && (VM_UGACTIVI("HASTA_DT").gt(lit("2017-07-01"))  ||  VM_UGACTIVI("HASTA_DT").isNull))
    ).select("UNGES_ID", "UGACT_ID", "UAACT_ID")



    //val VM_UGACTIVI2 = VM_UGACTIVI.filter((VM_UGACTIVI("DESDE_DT").lt(lit(date)) && (VM_UGACTIVI("DESDE_DT").gt(lit("2017-06-30")))) || (VM_UGACTIVI("DESDE_DT").lt(lit("2017-08-01")) && (VM_UGACTIVI("HASTA_DT").gt(lit("2017-07-01")) || VM_UGACTIVI("HASTA_DT").isNull)))



    val VM_UGACTMUN = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_UGACTMUN_20220624_174425.csv.bz2")

    val VM_UGACTMUN_FILTERED = VM_UGACTMUN.filter(
      (VM_UGACTMUN("DESDE_DT").lt(lit("2017-08-01")) && VM_UGACTMUN("DESDE_DT").gt(lit("2017-06-30")))
        || (VM_UGACTMUN("DESDE_DT").lt(lit("2017-07-01")) && (VM_UGACTMUN("HASTA_DT").gt(lit("2017-07-01"))  ||  VM_UGACTMUN("HASTA_DT").isNull))
    ).select("UGACT_ID", "ELMUN_ID")

    val VM_UNIDADMI = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_UNIDADMI_20220624_174438.csv.bz2")
      .select("UNADM_ID", "COMAU_ID")

    /**
    val VM_VOLUMENS = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("DWE_VM_VOLUMENS_20220714_125755.csv.bz2")
    */


    /** Creamos las vistas temporales para cuando queremos referenciar las tablas dentro de un spark.sql (véase la creación de OPER_UTE_UF, donde sin la vista temporal de SGR_MU_UTE y de SGR_MU_UF_FILTERED no funcionaría)*/

      //SGE_SAP.createOrReplaceTempView("SGE_SAP")
      SGR_MU_UTE.createOrReplaceTempView("SGR_MU_UTE")
      SGR_MU_UF_FILTERED.createOrReplaceTempView("SGR_MU_UF_FILTERED")
      //VM_COMUAUTO.createOrReplaceTempView("VM_COMUAUT")
      //VM_ELTREPOB_FILTERED.createOrReplaceTempView("VM_ELTREPOB_FILTERED")
      //VM_ENTLOCAL.createOrReplaceTempView("VM_ENTLOCAL")
      //VM_ENTLTPRE_FILTERED.createOrReplaceTempView("VM_ENTLTPRE_FILTERED")
      //VM_POBPERST.createOrReplaceTempView("VM_POBPERST")
      //VM_TIPOLENT_FILTERED.createOrReplaceTempView("VM_TIPOLENT_FILTERED")
      //VM_TIPOLFAC_FILTERED.createOrReplaceTempView("VM_TIPOLFAC_FILTERED")
      //VM_TPRECOGI.createOrReplaceTempView("VM_TPRECOGI")
      //VM_UAACTIVI_FILTERED.createOrReplaceTempView("VM_UAACTIVI_FILTERED")
      //VM_UFTRGMUN_FILTERED.createOrReplaceTempView("VM_UFTRGMUN_FILTERED")
      //VM_UFUGACTI_FILTERED.createOrReplaceTempView("VM_UFUGACTI_FILTERED")
      //VM_UGACTIVI_FILTERED.createOrReplaceTempView("VM_UGACTIVI_FILTERED")
      //VM_UGACTMUN_FILTERED.createOrReplaceTempView("VM_UGACTMUN_FILTERED")
      //VM_UNIDADMI.createOrReplaceTempView("VM_UNIDADMI")







    /**
    val R_JOIN_UFUGACTI_SAP =  SGE_SAP.alias("SGE_SAP").join(VM_UFUGACTI_FILTERED.alias("VM_UFUGACTI_FILTERED"), SGE_SAP("PROVE_ID") === VM_UFUGACTI_FILTERED("UNFAC_ID"), "right")
     */

    /** Join línea 20 */

      /** V */
    val R_JOIN_UFUGACTI_SAP = VM_UFUGACTI_FILTERED.alias("VM_UFUGACTI_FILTERED").join(SGE_SAP.alias("SGE_SAP"), VM_UFUGACTI_FILTERED("UNFAC_ID") === SGE_SAP("PROVE_ID"), "right")

     /**
    val R_JOIN_ENTLTPRE_TIPOLFAC = VM_ENTLTPRE_FILTERED.join(VM_TIPOLFAC_FILTERED, Seq("ELMUN_ID"), "right")
      */

    /** Joins de la línea 34 a la 37 */

      // PUEDE SER R?
     val R_JOIN_TIPOLFAC_ENTLTPRE = VM_TIPOLFAC_FILTERED.alias("VM_TIPOLFAC_FILTERED").join(VM_ENTLTPRE_FILTERED.alias("VM_ENTLTPRE_FILTERED"), /**VM_TIPOLFAC_FILTERED("ELMUN_ID") === VM_ENTLTPRE_FILTERED("ELMUN_ID")*/ Seq("ELMUN_ID"), "right") //Había un Seq("ELMUN_ID")

    val L_JOIN_UFUGACTI_UGACTIVI = VM_UFUGACTI_FILTERED.alias("VM_UFUGACTI_FILTERED").join(VM_UGACTIVI_FILTERED.alias("VM_UGACTIVI_FILTERED"), VM_UFUGACTI_FILTERED("UGACT_ID") === VM_UGACTIVI_FILTERED("UGACT_ID"), "left").select("UNFAC_ID", "VM_UFUGACTI_FILTERED.UGACT_ID", "UFUGA_ID", "VM_UFUGACTI_FILTERED.DESDE_DT", "VM_UFUGACTI_FILTERED.HASTA_DT", "UAACT_ID", "UNGES_ID")

    val L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB = L_JOIN_UFUGACTI_UGACTIVI.alias("L_JOIN_UFUGACTI_UGACTIVI").join(VM_ELTREPOB_FILTERED.alias("VM_ELTREPOB_FILTERED"), VM_UFUGACTI_FILTERED.alias("VM_UFUGACTI_FILTERED")("DESDE_DT") <= VM_ELTREPOB_FILTERED("HASTA_DT") &&
      (
        (VM_UFUGACTI_FILTERED.col("HASTA_DT").isNull && VM_ELTREPOB_FILTERED("DESDE_DT") >= VM_ELTREPOB_FILTERED.col("DESDE_DT")) ||
          (VM_UFUGACTI_FILTERED.col("HASTA_DT").isNotNull && VM_UFUGACTI_FILTERED.col("HASTA_DT") >= VM_ELTREPOB_FILTERED.col("DESDE_DT"))
        )
      , "left").select("UNFAC_ID", "UGACT_ID", "UFUGA_ID", "L_JOIN_UFUGACTI_UGACTIVI.DESDE_DT", "L_JOIN_UFUGACTI_UGACTIVI.HASTA_DT", "UAACT_ID", "UNGES_ID", "POBLA_QT", "POBIN_QT", "UFTRG_ID", "VERSI_ID")

    val R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES = L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB.alias("L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB").join(SGE_SAP.alias("SGE_SAP"), SGE_SAP("PROVE_ID") === VM_UFUGACTI_FILTERED.alias("VM_UFUGACTI_FILTERED")("UNFAC_ID"), "right") /** Es la misma fila siempre menos las columnas del final */



    /** SUBCONSULTA LÍNEA 50 */


    val OPER_UTE_UF = spark.sql(
      """select (1 * coalesce(OP.PORCENTAJE_QT, 100) / 100) * coalesce(OU.PORCENTAJE_QT, 100) / 100 AS POBDC_QT,
        |                       OP.OPERADOR_ID OPERADOR_ID_OP, OU.OPERADOR_ID OPERADOR_ID_OU,
        |             COALESCE(OP.OPERADOR_ID, OU.OPERADOR_ID, 0) AS OPERADOR_ID,
        |             CASE WHEN OP.OPERADOR_ID IS NOT NULL THEN OP.PORCENTAJE_QT
        |                  ELSE OU.PORCENTAJE_QT
        |             END PORCENTAJE_QT,
        |             coalesce(OP.UTE_ID,0) AS UTE_ID,
        |             CASE WHEN OP.UTE_ID IS NOT NULL THEN OP.PORCENTAJE_QT
        |                  ELSE NULL
        |             END AS PORCENTAJE_UTE_QT ,OP.DESDE_DT,OP.HASTA_DT, OP.UFUGA_ID,OP.MEDIOSPP_SN
        |             from SGR_MU_UTE OU  left join SGR_MU_UF_FILTERED OP on OP.UTE_ID = OU.UTE_ID""".stripMargin) // línea 35-47



    /** Join línea 51 OPER_UTE_UF con UFUGACTI */


    //val R_JOIN_OPER_UFUGACTI = OPER_UTE_UF.alias("OPER_UTE_UF").join(VM_UFUGACTI_FILTERED.alias("VM_UFUGACTI_FILTERED"), VM_UFUGACTI_FILTERED("UFUGA_ID") === OPER_UTE_UF("UFUGA_ID"), "right")
    val R_JOIN_OPER_UFUGACTI = OPER_UTE_UF.alias("OPER_UTE_UF").join(VM_UFUGACTI_FILTERED.alias("VM_UFUGACTI_FILTERED"), /**VM_UFUGACTI_FILTERED("UFUGA_ID") === OPER_UTE_UF("UFUGA_ID")*/ Seq("UFUGA_ID"), "right") //Había right




    /** WHERE línea 52 hasta 104 */

    val union_para_where = VM_UAACTIVI_FILTERED.alias("VM_UAACTIVI_FILTERED")
      .join(VM_UGACTIVI_FILTERED.alias("VM_UGACTIVI_FILTERED"), VM_UAACTIVI_FILTERED("UAACT_ID") === VM_UGACTIVI_FILTERED("UAACT_ID"), "inner")
      .join(VM_UGACTMUN_FILTERED.alias("VM_UGACTMUN_FILTERED"), VM_UGACTMUN_FILTERED("UGACT_ID") === VM_UGACTIVI_FILTERED("UGACT_ID"), "inner")
      .join(VM_ENTLOCAL.alias("VM_ENTLOCAL"), VM_ENTLOCAL("ELMUN_ID") === VM_UGACTMUN_FILTERED("ELMUN_ID"), "inner")
      .join(VM_UFUGACTI_FILTERED.alias("VM_UFUGACTI_FILTERED"), VM_UFUGACTI_FILTERED("UGACT_ID") === VM_UGACTIVI_FILTERED("UGACT_ID"), "inner")
      .join(VM_UFTRGMUN_FILTERED.alias("VM_UFTRGMUN_FILTERED"), VM_UFTRGMUN_FILTERED("UFUGA_ID") === VM_UFUGACTI_FILTERED("UFUGA_ID"), "inner")
      //.join(VM_ENTLTPRE_FILTERED.alias("VM_ENTLTPRE_FILTERED"), VM_ENTLTPRE_FILTERED("MUNTR_ID") === VM_UFTRGMUN_FILTERED("MUNTR_ID"), "inner")
      //.join(VM_ENTLTPRE_FILTERED, VM_ENTLOCAL("ELMUN_ID") === VM_ENTLTPRE_FILTERED("ELMUN_ID") , "inner")
      //.join(VM_UFTRGMUN_FILTERED, VM_UFTRGMUN_FILTERED("UFTRG_ID") === VM_ELTREPOB_FILTERED.alias("VM_ELTREPOB_FILTERED")("UFTRG_ID"), "inner")
      //.join(VM_UFTRGMUN_FILTERED, Seq("UFTRG_ID"), "inner")                                             //ENTLPRE O R_JOIN_TIPOLFAC_ENTLTPRE
      .join(R_JOIN_TIPOLFAC_ENTLTPRE.alias("R_JOIN_TIPOLFAC_ENTLTPRE"), VM_ENTLOCAL("ELMUN_ID") === R_JOIN_TIPOLFAC_ENTLTPRE("ELMUN_ID") && VM_UFTRGMUN_FILTERED("MUNTR_ID") === R_JOIN_TIPOLFAC_ENTLTPRE("MUNTR_ID"), "inner")
      .select("VM_UAACTIVI_FILTERED.UNADM_ID", "VM_UAACTIVI_FILTERED.ACTIV_ID", "VM_UGACTIVI_FILTERED.UNGES_ID", "VM_ENTLOCAL.ELMUN_ID", "VM_UFTRGMUN_FILTERED.MUNTR_ID", "VM_UFTRGMUN_FILTERED.UFTRG_ID", "VM_UFUGACTI_FILTERED.UNFAC_ID", "VM_UFUGACTI_FILTERED.UFUGA_ID", "R_JOIN_TIPOLFAC_ENTLTPRE.TPREC_ID", "VM_UGACTIVI_FILTERED.UGACT_ID", "R_JOIN_TIPOLFAC_ENTLTPRE.TPGFA_ID") //"VM_UFTRGMUN_FILTERED.MUNTR_ID", "VM_UFTRGMUN_FILTERED.UFTRG_ID", "VM_ENTLTPRE_FILTERED.TPREC_ID", "VM_ELTREPOB_FILTERED.DESDE_DT")



    /**
    val datos = VM_UAACTIVI2.alias("VM_UAACTIVI2")
    .join(VM_UGACTIVI2.alias("VM_UGACTIVI2"), VM_UAACTIVI2("UAACT_ID") === VM_UGACTIVI2("UAACT_ID"), "inner") // U-G
    .join(VM_UGACTMUN2.alias("VM_UGACTMUN2"), VM_UGACTMUN2("UGACT_ID") === VM_UGACTIVI2("UGACT_ID"), "inner") // G-M
    .join(VM_ENTLOCAL.alias("VM_ENTLOCAL"), VM_ENTLOCAL("ELMUN_ID") === VM_UGACTMUN2("ELMUN_ID"), "inner") // M-L
    .join(VM_UFUGACTI2.alias("VM_UFUGACTI2"), VM_UFUGACTI2("UGACT_ID") === VM_UGACTIVI2("UGACT_ID"), "inner") // G-F
    .join(VM_UFTRGMUN2.alias("VM_UFTRGMUN2"), VM_UFTRGMUN2("UFUGA_ID") === VM_UFUGACTI2("UFUGA_ID"), "inner") // F-T
    .select("VM_UAACTIVI2.UNADM_ID", "VM_UAACTIVI2.ACTIV_ID", "VM_UGACTIVI2.UNGES_ID", "VM_ENTLOCAL.ELMUN_ID", "VM_UFTRGMUN2.MUNTR_ID", "VM_UFTRGMUN2.UFTRG_ID", "VM_UFUGACTI2.UNFAC_ID", "VM_UFUGACTI2.UFUGA_ID", "VM_UFUGACTI2.UNFAC_ID")
     */


    val siguiente = union_para_where.alias("union_para_where")
      .join(VM_ELTREPOB_FILTERED.alias("VM_ELTREPOB_FILTERED"), VM_ELTREPOB_FILTERED("UFTRG_ID") === union_para_where("UFTRG_ID"), "inner")
      /**
      .join(VM_POBPERST.alias("VM_POBPERST"), VM_POBPERST("UFUGA_ID") === union_para_where("UFUGA_ID"), "inner")
      */
      //.join(VM_POBPERST, VM_POBPERST("DESDE_DT") === VM_ELTREPOB_FILTERED("DESDE_DT"), "inner") //NOS DA ERROR DESDE_DT AMBIGUO, CREEMOS QUE NO HACE FALTA PQ YA TENEMOS UN DESDE_DT
      .join(VM_UNIDADMI.alias("VM_UNIDADMI"), VM_UNIDADMI("UNADM_ID") === union_para_where("UNADM_ID"), "inner")
      .join(VM_COMUAUTO.alias("VM_COMUAUTO"), VM_COMUAUTO("COMAU_ID") === VM_UNIDADMI("COMAU_ID"), "inner")
      .join(VM_TPRECOGI.alias("VM_TPRECOGI"),  VM_TPRECOGI("TPREC_ID") === R_JOIN_TIPOLFAC_ENTLTPRE("TPREC_ID"), "inner")
      .join(VM_TIPOLENT_FILTERED.alias("VM_TIPOLENT_FILTERED"), VM_TIPOLENT_FILTERED("ELMUN_ID") === union_para_where("ELMUN_ID"), "inner")
      .select("union_para_where.UNADM_ID", "union_para_where.ACTIV_ID", "union_para_where.UNGES_ID", "union_para_where.ELMUN_ID", "union_para_where.MUNTR_ID", "union_para_where.UFTRG_ID", "union_para_where.UNFAC_ID", "union_para_where.UFUGA_ID", "union_para_where.TPREC_ID", "union_para_where.UGACT_ID", "union_para_where.TPGFA_ID", "VM_ELTREPOB_FILTERED.DESDE_DT", /**"VM_POBPERST.UFUGA_ID",*/ "VM_TIPOLENT_FILTERED.TPENT_ID", "VM_TPRECOGI.PROCE_ID")




   // R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES.show()


    val siguiente_2 = siguiente.alias("siguiente")
      .join(R_JOIN_OPER_UFUGACTI.alias("R_JOIN_OPER_UFUGACTI"), R_JOIN_OPER_UFUGACTI("UFUGA_ID") === siguiente("UFUGA_ID"), "inner")
      .join(R_JOIN_OPER_UFUGACTI, Seq("UGACT_ID"), "inner")
      //.join(R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES.alias("UFU_UGAC_ELTREPOB_SGE"), R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES("UFUGA_ID") === siguiente("UFUGA_ID"), "inner")
     // .join(R_JOIN_OPER_UFUGACTI.alias("R_JOIN_OPER_UFUGACTI"), R_JOIN_OPER_UFUGACTI("OP.UFUGA_ID") === siguiente("ufuga_id"), "left")
      .select("siguiente.UNADM_ID", "siguiente.ACTIV_ID", "siguiente.UNGES_ID", "siguiente.ELMUN_ID", "siguiente.MUNTR_ID", "siguiente.UFTRG_ID", "siguiente.UNFAC_ID", "siguiente.UFUGA_ID", "siguiente.TPREC_ID", "siguiente.UGACT_ID","siguiente.DESDE_DT", "siguiente.TPENT_ID", "siguiente.TPGFA_ID", "siguiente.PROCE_ID", "siguiente.TPGFA_ID", "R_JOIN_OPER_UFUGACTI.POBDC_QT", "R_JOIN_OPER_UFUGACTI.OPERADOR_ID", "R_JOIN_OPER_UFUGACTI.PORCENTAJE_QT", "R_JOIN_OPER_UFUGACTI.UTE_ID", "R_JOIN_OPER_UFUGACTI.MEDIOSPP_SN", "R_JOIN_OPER_UFUGACTI.OPERADOR_ID_OP", "R_JOIN_OPER_UFUGACTI.OPERADOR_ID_OU")
      //.select("siguiente.DESDE_DT","siguiente.UNADM_ID", "siguiente.ACTIV_ID", "siguiente.UNGES_ID", "siguiente.ELMUN_ID","siguiente.UNFAC_ID","siguiente.TPREC_ID","siguiente.TPENT_ID", "siguiente.TPGFA_ID", "siguiente.PROCE_ID", /**"R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES.PROVE_NM",*/ "R_JOIN_OPER_UFUGACTI.OPERADOR_ID", /**"R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES.POBIN_QT",*/ "siguiente.POBDC_QT", /**"R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES.POBLA_QT",*/ "siguiente.PORCENTAJE_QT", "R_JOIN_OPER_UFUGACTI.UTE_ID", "R_JOIN_OPER_UFUGACTI.MEDIOSPP_SN", "siguiente.MUNTR_ID", "siguiente.UFTRG_ID", "siguiente.UFUGA_ID", "siguiente.UGACT_ID")
   // R_JOIN_UFUGACTI_SAP.show()
    //R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES.show()


    val siguiente_3 = siguiente_2.alias("siguiente_2")
      .join(R_JOIN_UFUGACTI_SAP, R_JOIN_UFUGACTI_SAP("UFUGA_ID") === siguiente_2("UFUGA_ID"), "inner") // Para tener la tabla R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES y poder hacer el join de abajo
      .join(R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES.alias("UFU_UGA_ELTRE"), R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES("PROVE_ID") === R_JOIN_UFUGACTI_SAP("PROVE_ID"), "inner")
      //.join(R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES.alias("UFU_UGAC_ELTREPOB_SGE"), R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES("UFUGA_ID") === siguiente("UFUGA_ID"), "inner")
      //.join(total.alias("total"), total("OP.UFUGA_ID") === datos("ufuga_id"), "left")
      //.select("siguiente_2.UNADM_ID", "siguiente_2.ACTIV_ID", "siguiente_2.UNGES_ID", "siguiente_2.ELMUN_ID", "siguiente_2.MUNTR_ID", "siguiente_2.UFTRG_ID", "siguiente_2.UNFAC_ID", "siguiente_2.UFUGA_ID", "siguiente_2.TPREC_ID", "siguiente_2.UGACT_ID","siguiente_2.DESDE_DT", "siguiente_2.TPENT_ID", "siguiente_2.POBDC_QT")
      .select("siguiente_2.DESDE_DT","siguiente_2.UNADM_ID", "siguiente_2.ACTIV_ID", "siguiente_2.UNGES_ID", "siguiente_2.ELMUN_ID","siguiente_2.UNFAC_ID","siguiente_2.TPREC_ID","siguiente_2.TPENT_ID", "siguiente_2.TPGFA_ID", "siguiente_2.PROCE_ID", "UFU_UGA_ELTRE.PROVE_NM", "siguiente_2.OPERADOR_ID", "UFU_UGA_ELTRE.POBIN_QT", "siguiente_2.POBDC_QT", "UFU_UGA_ELTRE.POBLA_QT", "siguiente_2.PORCENTAJE_QT", "siguiente_2.UTE_ID", "siguiente_2.MEDIOSPP_SN", "siguiente_2.MUNTR_ID", "siguiente_2.UFTRG_ID", "siguiente_2.UFUGA_ID", "siguiente_2.UGACT_ID", "siguiente_2.OPERADOR_ID_OP", "siguiente_2.OPERADOR_ID_OU")
    // .groupBy("siguiente_2.DESDE_DT","siguiente_2.UNADM_ID", "siguiente_2.ACTIV_ID", "siguiente_2.UNGES_ID", "siguiente_2.ELMUN_ID","siguiente_2.UNFAC_ID","siguiente_2.TPREC_ID","siguiente_2.TPENT_ID", "siguiente_2.TPGFA_ID", "siguiente_2.PROCE_ID", /**"R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES.PROVE_NM",*/ "siguiente_2.OPERADOR_ID", /**"R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES.POBIN_QT",*/ "siguiente_2.POBDC_QT", /**"R_JOIN_L_JOIN_UFUGACTI_UGACTIVI_ELTREPOB_PROVEEDORES.POBLA_QT",*/ "siguiente_2.PORCENTAJE_QT", "siguiente_2.UTE_ID", "siguiente_2.MEDIOSPP_SN", "siguiente_2.MUNTR_ID", "siguiente_2.UFTRG_ID", "siguiente_2.UFUGA_ID", "siguiente_2.UGACT_ID")
      


    var total3 = siguiente_3.withColumn("POBDC_QT", when(col("POBDC_QT").isNull, 0).otherwise(col("POBDC_QT"))) //PROCE_ID
      .withColumn("PROCE_ID", when(col("PROCE_ID").isNull, 0).otherwise(col("PROCE_ID"))) //PROCE_ID
      .withColumn("OPERADOR_ID", when(col("OPERADOR_ID").isNull, 0).otherwise(col("OPERADOR_ID"))) //PROCE_ID
      .withColumn("POBDC_QT", when(col("POBDC_QT") === 0, 1).otherwise(col("POBDC_QT"))) //PROCE_ID


    var total4 =
      total3.
        withColumn("POBGC_QT", col("POBLA_QT") * col("POBDC_QT"))
        .withColumn("POBDC_QT_2", col("POBIN_QT") * col("POBDC_QT"))
        //.select("DESDE_DT", "UNADM_ID", "ACTIV_ID", "UNGES_ID", "ELMUN_ID", "UFTRG_ID", "UNFAC_ID", "TPREC_ID", "TPENT_ID", "PROCE_ID", "UFUGA_ID", "PROVE_NM", "POBDC_QT", "POBGC_QT", "OPERADOR_ID_OP", "OPERADOR_ID_OU", "OPERADOR_ID", "PORCENTAJE_QT", "POBDC_QT_2")
        //.groupBy("DESDE_DT", "UNADM_ID", "ACTIV_ID", "UNGES_ID", "ELMUN_ID", "UFTRG_ID", "UNFAC_ID", "TPREC_ID", "TPENT_ID", "PROCE_ID", "UFUGA_ID", "PROVE_NM", "POBDC_QT", "POBGC_QT", "OPERADOR_ID_OP", "OPERADOR_ID_OU", "OPERADOR_ID", "PORCENTAJE_QT", "POBDC_QT_2")
        .select("DESDE_DT", "UNADM_ID", "ACTIV_ID", "UNGES_ID", "ELMUN_ID", "UNFAC_ID", "TPREC_ID", "TPENT_ID", "TPGFA_ID", "PROCE_ID", "POBIN_QT", "POBLA_QT", "OPERADOR_ID_OU", "OPERADOR_ID", "PROVE_NM", "POBDC_QT", "PORCENTAJE_QT", "MEDIOSPP_SN")
        .groupBy("DESDE_DT", "UNADM_ID", "ACTIV_ID", "UNGES_ID", "ELMUN_ID", "UNFAC_ID", "TPREC_ID", "TPENT_ID", "TPGFA_ID", "PROCE_ID", "POBIN_QT", "POBLA_QT", "OPERADOR_ID_OU", "OPERADOR_ID", "PROVE_NM", "POBDC_QT", "PORCENTAJE_QT", "MEDIOSPP_SN")
        .count()
        .show()





    //total4.filter($"POBGC_QT" === "9263").show()





    /** "VM_ENTLOCAL.ELMUN_ID" */

/**
    val union2_para_where = union_para_where.alias("union_para_where")


      .select("union_para_where.UNADM_ID", "union_para_where.ACTIV_ID", "union_para_where.UNGES_ID", "union_para_where.UNFAC_ID", "union_para_where.UFUGA_ID", "VM_UFTRGMUN_FILTERED.MUNTR_ID", "VM_UFTRGMUN_FILTERED.UFTRG_ID", "VM_ENTLTPRE_FILTERED.TPREC_ID", "VM_ELTREPOB_FILTERED.DESDE_DT")


    union2_para_where.show()
 */







    /** SELECT línea 98 */
   // val SELECT_LINEA_98 = VM_ELTREPOB_FILTERED.select(max("VERSI_ID"))













    /** Ejemplo de como hacer el select tocho que no nos va */
      /**
    val prueba_Select = VM_UAACTIVI_FILTERED.select("UNADM_ID", "ACTIV_ID").where(VM_UAACTIVI_FILTERED.col("UAACT_ID") === VM_UGACTIVI_FILTERED.col("UAACT_ID")).show()
*/
    /**
      groupBy(VM_UAACTIVI_FILTERED.col("UNADM_ID"))
 */

  }



  def CargaMedios(): Unit = {









  }


  def CargaKilos():Unit={}
  //creacion de la tabla kilos


}
