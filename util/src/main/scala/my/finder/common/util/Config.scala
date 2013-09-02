package my.finder.common.util

import com.typesafe.config._

/**
 * 读取配置文件
 */
object Config {
  private val conf = ConfigFactory.load
  def apply[A](s:String):A = {
  	conf.getAnyRef(s).asInstanceOf[A]
  }
}
