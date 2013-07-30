package my.finder.index.service

import com.jolbox.bonecp.BoneCPDataSource
import java.sql.{ResultSet, Statement, Connection}
import my.finder.common.util.Config
import javax.sql.DataSource
import edu.fudan.nlp.cn.tag.CWSTagger
import edu.fudan.ml.types.Dictionary

import scala.collection.mutable.HashMap
import org.slf4j.LoggerFactory
import scala.util.control.Breaks._

object DBService {
  val log = LoggerFactory.getLogger("my")
  private var _ds: DataSource = null
  private var _tag: CWSTagger = null
  val productTypesEn = new HashMap[String, String]()
  val productTypesRu = new HashMap[String, String]()
  val productTypesBr = new HashMap[String, String]()

  def init = {
    Class.forName(Config.get("dbDriver"))
    val ds = new BoneCPDataSource()
    ds.setJdbcUrl(Config.get("dbUrl"))
    ds.setUsername(Config.get("dbUser"))
    ds.setPassword(Config.get("dbPassword"))
    _ds = ds

    _tag = new CWSTagger(Config.get("tag"))
    val dictionary = new Dictionary(Config.get("dict"));
    _tag.setDictionary(dictionary);
    loadProductTypeDatas
  }

  def loadProductTypeDatas {
    log.info("loading ProductType datas")
    productTypesEn.clear()
    productTypesRu.clear()
    productTypesBr.clear()
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    try {
      conn = DBService.dataSource.getConnection()
      stmt = conn.createStatement()
      val sql = "select producttypeid_int,producttypealiasname_nvarchar ,indexcode_nvarchar  from ec_producttype "
      val typeRuSql = "select ProductTypeID_int,AliasName_nvarchar from MKT_TYPE_MONTH_AREASALES where WebSiteID_smallint = 61 and AreaCode_nvarchar = 'ru'"
      val typeBrSql = "select ProductTypeID_int,AliasName_nvarchar from MKT_TYPE_MONTH_AREASALES where WebSiteID_smallint = 61 and AreaCode_nvarchar = 'br'"
      rs = stmt.executeQuery(typeRuSql)
      val ruTypes = new HashMap[Int, String]()
      while(rs.next()){
        ruTypes += (rs.getInt("ProductTypeID_int") -> rs.getString("AliasName_nvarchar"))
      }

      rs = stmt.executeQuery(typeBrSql)
      val brTypes = new HashMap[Int, String]()
      while(rs.next()){
        brTypes += (rs.getInt("ProductTypeID_int") -> rs.getString("AliasName_nvarchar"))
      }

      rs = stmt.executeQuery(sql)
      while (rs.next()) {
        productTypesEn += (rs.getString("indexcode_nvarchar") -> rs.getString("producttypealiasname_nvarchar"))
        try{
          productTypesRu += (rs.getString("indexcode_nvarchar") -> ruTypes(rs.getInt("producttypeid_int")))
        } catch {
          case e:Exception =>
        }
        try{
          productTypesBr += (rs.getString("indexcode_nvarchar") -> brTypes(rs.getInt("producttypeid_int")))
        } catch {
          case e:Exception =>
        }
        
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      if (rs != null) rs.close()
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
    log.info("loaded ProductType datas")
  }

  def dataSource = {
    _ds
  }

  def tag = {
    _tag
  }
}