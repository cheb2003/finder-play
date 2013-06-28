package my.finder.index.service

import com.jolbox.bonecp.BoneCPDataSource
import java.sql.Connection
import my.finder.common.util.Config
import javax.sql.DataSource
import edu.fudan.nlp.cn.tag.CWSTagger
import edu.fudan.ml.types.Dictionary

object DBService{
	private var _ds:DataSource = null
  private var _tag:CWSTagger = null
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

	}
	def dataSource = {
		_ds
	}
  def tag = {
    _tag
  }
}