package my.finder.console.service


import java.sql.{ResultSet, Statement, Connection, DriverManager}
import com.jolbox.bonecp.BoneCPDataSource
import javax.sql.DataSource
import my.finder.common.util.Config

object DBMysql {

  private var _ds: DataSource = null
  private val driver = Config[String]("mysqlDBDriver")
  private val url = Config[String]("mysqlDBUrl")
  private val user = Config[String]("mysqlDBUser")
  private val password = Config[String]("mysqlDBPassword")
  def init = {
    Class.forName(driver)
    val ds = new BoneCPDataSource()
    ds.setJdbcUrl(url)
    ds.setUsername(user)
    ds.setPassword(password)
    _ds = ds
  }
  def ds:DataSource = {
    _ds
  }
  def colseConn(conn:Connection, stem:Statement, rs:ResultSet) = {
    if ( conn != null ){
      conn.close()
    }
    if ( stem != null ){
      stem.close()
    }
    if ( rs != null ){
      rs.close()
    }
  }
 }
