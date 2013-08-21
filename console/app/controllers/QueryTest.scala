package controllers

import com.jolbox.bonecp.BoneCPDataSource
import java.sql.{ResultSetMetaData, ResultSet, Statement, Connection}
import javax.sql.DataSource
import scala.collection.mutable.Queue
import org.slf4j.LoggerFactory
import play.api.mvc._
import play.api.data._
import play.api.data.Forms._
import play.api.Play._
import my.finder.common.util.Util
import scala.xml.{Null, Text, Attribute, Node}

object QueryTest extends Controller{
  val log = LoggerFactory.getLogger("my")
  private var _ds: DataSource = null

  def query = Action { implicit request =>
    val form = Form(
      "q" -> text
    )
    val queryParams = form.bindFromRequest.data
    val sql = Util.getParamString(queryParams, "q", "")
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    val nodes = new Queue[Node]()
    var count = 0
    try {
      conn = QueryTest.dataSource.getConnection()
      stmt = conn.createStatement()
      //val sql = "select ProductID_int,ProductAliasName_nvarchar,ProductPrice_money from ec_product "
      rs = stmt.executeQuery(sql)
      val rmta: ResultSetMetaData = rs.getMetaData
      count = rmta.getColumnCount
      while (rs.next()) {
        var n = <doc/>
        for(i <- 1 to count) {
          var colname = rmta.getColumnName(i)
          val col = "col"
          if(colname == "" || colname == null) {
            colname = col + i
          }
          n = n % Attribute(None, colname, Text(rs.getString(i)), Null)
        }
        nodes += n
      }
      Ok(<root>{nodes}</root>)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        Ok("sql有误，查询失败！")
      }
    } finally {
      if (rs != null) rs.close()
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }

  }

  def init = {
    Class.forName(current.configuration.getString("mssqlDBDriver").get)
    val ds = new BoneCPDataSource()
    ds.setJdbcUrl(current.configuration.getString("mssqlDBUrl").get)
    ds.setUsername(current.configuration.getString("mssqlDBUser").get)
    ds.setPassword(current.configuration.getString("mssqlDBPassword").get)
    _ds = ds
  }

  /*def productDatas(sql: String):Queue[Node] = {
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    try {
      conn = QueryTest.dataSource.getConnection()
      stmt = conn.createStatement()
      //val sql = "select ProductID_int,ProductAliasName_nvarchar,ProductPrice_money from ec_product "
      rs = stmt.executeQuery(sql)
      val nodes = new Queue[Node]()
      var n = <doc/>
      while (rs.next()) {
        n = n % Attribute(None, "pid", Text(String.valueOf(rs.getInt(1))), Null)
        n = n % Attribute(None, "pname", Text(rs.getString(2)), Null)
        n = n % Attribute(None, "pprice", Text(String.valueOf(rs.getDouble(3))), Null)
        println(rs.getInt(1)+"=========="+rs.getString(2)+"==================="+rs.getString(3))
        nodes += n
      }
      nodes
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      if (rs != null) rs.close()
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }

  }
*/

  def dataSource = {
    if(_ds == null) {
      init
    }
    _ds
  }

}