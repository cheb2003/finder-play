package my.finder.common.util

import java.text.SimpleDateFormat
import java.util.Date
import play.api.Play.current
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
  def getParamString(v:Map[String,Any],key:String,default:String):String = {
    if(v.contains(key)) v(key).asInstanceOf[String] else default
  }
   
  def getParamInt(v:Map[String,Any],key:String,default:Int):Int = {
      if(v.contains(key)) Integer.valueOf(v(key).toString) else default
  }

  def getProfile:String = {
    current.configuration.getString("profile").get
  }

  def getProfileFromMyConfig:String = {
    Config.get("profile")
  }

  def getConsoleRootAkkaURLFromMyConfig:String = {
    if(getProfileFromMyConfig == Constants.PROFILE_PRODUCTION)
      "akka://console@127.0.0.1:2552/user/root"
    else if(getProfileFromMyConfig == Constants.PROFILE_TEST)
      "akka://console@127.0.0.1:3552/user/root"
    else
      null
  }


  def getIndexRootAkkaURL:String = {
    if(getProfile == Constants.PROFILE_PRODUCTION) 
      "akka://index@127.0.0.1:2554/user/root"
    else if(getProfile == Constants.PROFILE_TEST)
      "akka://index@127.0.0.1:3554/user/root"
    else 
      null
  }

  def getIndexManagerAkkaURL:String = {
    if(getProfile == Constants.PROFILE_PRODUCTION) 
      "akka://console@127.0.0.1:2552/user/root/indexManager"
    else if(getProfile == Constants.PROFILE_TEST)
      "akka://console@127.0.0.1:3552/user/root/indexManager"
    else 
      null
  }
  
  def getConsoleRootAkkaURL:String = {
    if(getProfile == Constants.PROFILE_PRODUCTION) 
      "akka://console@127.0.0.1:2552/user/root"
    else if(getProfile == Constants.PROFILE_TEST)
      "akka://console@127.0.0.1:3552/user/root"
    else
      null
  }

  def getSize(queryParams: Map[String, String],default:Int = 20) = {
    val size = getParamInt(queryParams, "size", default)
    if(size < 0 || size > 100) 20 else size
  }
  def getPage(queryParams: Map[String, String],default:Int = 1) = {
    val page = getParamInt(queryParams, "page", default)
    if(page < 0) 1 else page
  }
}
