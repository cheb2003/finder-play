
import my.finder.search.service.SearcherManager
import play.api._

/**
 *
 */
object Global extends GlobalSettings {


  override def onStart(app: Application) {
    SearcherManager.init
  }

  override def onStop(app: Application) {

  }
}
