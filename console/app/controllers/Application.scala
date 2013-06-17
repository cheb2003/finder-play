package controllers

import play.api.mvc._
import play.api.libs.json.{Json, JsValue}
import my.finder.common.message.{IndexIncremetionalTaskMessage, CommandParseMessage}
import my.finder.common.util.{Constants}
import my.finder.console.actor.MessageFacade.rootActor




object Application extends Controller {
  val json: JsValue = Json.parse("""
{
  "user": {
    "name" : "toto",
    "age" : 25,
    "email" : "toto@jmail.com",
    "isAlive" : true,
    "friend" : {
  	  "name" : "tata",
  	  "age" : 20,
  	  "email" : "tata@coldmail.com"
    }
  }
}
                                 """)
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
    //Ok("hello world")
  }
  def hello(name:String) = Action {implicit request =>

    rootActor ! CommandParseMessage(Constants.DD_PRODUCT)
    Ok("hello")
  }

  def inc = Action {implicit request =>
    rootActor ! IndexIncremetionalTaskMessage("",null)
    Ok("inc")
  }
  
}