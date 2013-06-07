package my.finder.common.util

import java.text.SimpleDateFormat
import java.util.Date

/**
 *
 */
object Util {
  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
  def getKey(name: String, date: Date) = {
    name + "_" + sdf.format(date)
  }
  def getIncrementalPath(name: String, date: Date) = {
    name + "_" + sdf.format(date) + "_inc"
  }
  def getPrefixPath(workDir:String,key:String) = {
    workDir + "/" + key + "/"
  }
  def dateParseToString(date:Date):String = {
    val s = sdf.format(date)
    s
  }
  def stringParseToDate(str:String):Date = {
    var d:Date = null
    try{
      d = sdf.parse(str)
    } catch {
      case e:Exception => println("------------" + str)
    }
    d
  }
}
