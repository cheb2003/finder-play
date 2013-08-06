package my.finder.index.service

import com.jolbox.bonecp.BoneCPDataSource
import java.sql.{ResultSet, Statement, Connection}
import javax.sql.DataSource
import scala.collection.mutable.{ListBuffer}
import org.slf4j.LoggerFactory
import play.api.Play._

case class DDInfo(maxid: Int, minid: Int)
object DDService {
  val log = LoggerFactory.getLogger("my")
  private var _ds: DataSource = null
  //val productTypes = new HashMap[String, String]()

  def init = {
    Class.forName(current.configuration.getString("mssqlDBDriver").get)
    val ds = new BoneCPDataSource()
    ds.setJdbcUrl(current.configuration.getString("mssqlDBUrl").get)
    ds.setUsername(current.configuration.getString("mssqlDBUser").get)
    ds.setPassword(current.configuration.getString("mssqlDBPassword").get)
    _ds = ds

    //loadProductTypeDatas
  }

  def loadProductTypeDatas(): List[DDInfo] = {
    val set:ListBuffer[DDInfo] = new ListBuffer[DDInfo]
    log.info("loading ProductType datas")
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    try {
      conn = DDService.dataSource.getConnection()
      stmt = conn.createStatement()
      var sql = "select id,sku from ec_product "
      rs = stmt.executeQuery(sql)
      while (rs.next()) {
        set += DDInfo(rs.getInt(1),rs.getInt(2))
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
    set.toList
  }

  def dataSource = {
    if(_ds == null) {
      init
    }
    _ds
  }

}