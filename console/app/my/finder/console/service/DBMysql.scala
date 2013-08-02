package my.finder.console.service

import play.api.Play.current
import java.sql.{ResultSet, Statement, Connection, DriverManager}
import com.jolbox.bonecp.BoneCPDataSource
import javax.sql.DataSource

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 13-7-25
 * Time: 上午10:02
 * To change this template use File | Settings | File Templates.
 */
object DBMysql {

  private var _ds: DataSource = null
  private val driver = current.configuration.getString("mysqlDBDriver").get
  private val url = current.configuration.getString("mysqlDBUrl").get
  private val user = current.configuration.getString("mysqlDBUser").get
  private val password = current.configuration.getString("mysqlDBPassword").get
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
