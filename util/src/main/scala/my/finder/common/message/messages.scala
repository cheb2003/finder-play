package my.finder.common.message

import java.util.Date
import scala.collection.mutable.ListBuffer

case class IndexTaskMessage(name:String,date:Date,seq:Long,minId:Int,maxId:Int,batchSize:Int);
case class IndexAttributeTaskMessage(name:String,date:Date,seq:Long,minId:Int,maxId:Int,batchSize:Int,ddProductIndex:String)
case class IndexTaskMessageDD(name:String,date:Date,seq:Long,minId:Int,maxId:Int,total:Long,batchSize:Int);
case class IndexIncremetionalTaskMessage(name:String,date:Date);
case class OldIndexIncremetionalTaskMessage(name:String,date:Date);
case class PartitionIndexTaskMessage(name:String)
case class PartitionIndexAttributesTaskMessage(name:String,ddProductIndex:String)
case class CommandParseMessage(command:String)
case class CompleteSubTask(name:String,date:Date,seq:Long,successCount:Int,failCount:Int,skipCount:Int)
case class CreateSubTask(name:String,date:Date,total:Long)
case class ManageSubTask(name:String,date:Date)
case class CompleteIndexTask(name:String,date:Date)
case class CompleteIncIndexTask(name:String,date:Date,successCount:Int,failCount:Int,skipCount:Int)
case class CloseWriter(name:String,date:Date)
case class MergeIndexMessage(name:String,date:Date)
case class ChangeIndexMessage(name:String,date:Date)
case class CloseIndexWriterMessage(name:String,date:Date)
case class GetIndexesPathMessage()
case class IncIndexeMessage(name:String,date:Date)
case class GetIndexesPathMessageReponse(msg:List[String])

