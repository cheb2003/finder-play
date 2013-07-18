package my.finder.index.service

import com.jolbox.bonecp.BoneCPDataSource
import java.sql.{ResultSet, Statement, Connection}
import my.finder.common.util.Config
import javax.sql.DataSource
import edu.fudan.nlp.cn.tag.CWSTagger
import edu.fudan.ml.types.Dictionary

import scala.collection.mutable.HashMap
import org.slf4j.LoggerFactory

object DBService {
  val log = LoggerFactory.getLogger("my")
  private var _ds: DataSource = null
  private var _tag: CWSTagger = null
  val productTypes = new HashMap[String, String]()

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
    productTypes.clear()
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    try {
      conn = DBService.dataSource.getConnection()
      stmt = conn.createStatement()
      val sql = "select producttypealiasname_nvarchar ,indexcode_nvarchar  from ec_producttype "
      rs = stmt.executeQuery(sql)
      while (rs.next()) {
        productTypes += (rs.getString("indexcode_nvarchar") -> rs.getString("producttypealiasname_nvarchar"))
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