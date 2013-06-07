package my.finder.common.util

import java.util.Properties

/**
 *
 */
object Config {
  val p:Properties = new Properties()
  def init(s:String) = {
    val in = this.getClass.getClassLoader.getResourceAsStream(s)
    p.load(in)
    in.close()
  }
  def get(key:String) = {
    p.getProperty(key)
  }
}

