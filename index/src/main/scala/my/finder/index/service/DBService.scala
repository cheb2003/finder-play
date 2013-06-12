package my.finder.index.service

import com.jolbox.bonecp.BoneCPDataSource
import java.sql.Connection
import my.finder.common.util.Config
import javax.sql.DataSource
object DBService{
	private var _ds:DataSource = null
	def init = {
		Class.forName(Config.get("dbDriver"))
	 	val ds = new BoneCPDataSource()
	 	ds.setJdbcUrl(Config.get("dbUrl"))
		ds.setUsername(Config.get("dbUser"))
		ds.setPassword(Config.get("dbPassword"))
		_ds = ds
	}
	def dataSource = {
		_ds
	}
}