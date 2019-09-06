import com.rabbitmq.client.*
import com.rabbitmq.client.impl.ForgivingExceptionHandler
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.mainBody
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils.create
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.*
import java.util.*
import java.util.logging.FileHandler
import java.util.logging.Level
import java.util.logging.Logger
import java.util.logging.SimpleFormatter


/**
 * Created by Neverland on 18.01.2018.
 */

object GitProject : Table() {
    var id = integer("id").primaryKey().autoIncrement()
    var lastDownloadDate = long("last_download_date").nullable()
    var url = varchar("url", 250)
    var charset = varchar("charset", 250)
    var name = varchar("name", 250)
    var downloadedProjectVersionId = (integer("downloaded_project_version_id")).nullable()
}

object GitProjectVersion : Table() {
    var id = integer("id").primaryKey().autoIncrement()
    var gitProjectId = integer("project_id") references GitProject.id
    var versionHash = varchar("version_hash", 250)
}

object GitProjectExploredLibrary : Table() {
    var id = integer("id").primaryKey().autoIncrement()
    var status = bool("status")
    var gitProjectVersionId = integer("project_version_id") references GitProjectVersion.id
    var exploredLibraryVersionId = integer("library_version_id") references ExploredLibraryVersion.id
}

object ExploredLibraryVersion : Table() {
    var id = integer("id").primaryKey().autoIncrement()
    var exploredLibraryId = integer("library_id") references ExploredLibrary.id
    var version = varchar("version", 250)
}

object ExploredLibrary : Table() {
    var id = integer("id").primaryKey().autoIncrement()
    var name = varchar("name", 250)
    var groupName = varchar("group_name", 250)
}

val TAG = "repoMinerDownloader: "

val TASKS_QUEUE_NAME = "repositoryDownloadTasksQueue";
val ACK_QUEUE_NAME = "ackQueue";
var channel: Channel? = null
var responseChannel: Channel? = null

var messageCounter = 0

class MyArgs(parser: ArgParser) {

    val logger by parser.storing("logger's system path")
    val db by parser.storing("name of the database to store information about projects")
    val user by parser.storing("login for database to store information about projects")
    val password by parser.storing("password for database to store information about projects")

    val folder by parser.storing("system folder to store projects' code")

}

var fileLogger: Logger? = null
var fileHandler: FileHandler? = null

var realJavaReposCounter = 0

fun main(args: Array<String>) {

    var parsedArgs: MyArgs? = null

    mainBody {
        parsedArgs = ArgParser(args).parseInto(::MyArgs)
    }

    fileLogger = Logger.getLogger(TAG + "Log")

    try {

        fileHandler = FileHandler(parsedArgs!!.logger)
        fileLogger!!.addHandler(fileHandler)
        val formatter = SimpleFormatter()
        fileHandler!!.formatter = formatter

    } catch (e: SecurityException) {
        e.printStackTrace()
    } catch (e: IOException) {
        e.printStackTrace()
    }

    val jdbc = "jdbc:mysql://10.100.174.242:3306/${parsedArgs!!.db}?serverTimezone=UTC"
    val driver = "com.mysql.cj.jdbc.Driver"

    val dbConnection = Database.connect(jdbc, user = parsedArgs!!.user, password = parsedArgs!!.password, driver = driver)

    transaction {
        create(GitProject, GitProjectVersion, ExploredLibrary, ExploredLibraryVersion, GitProjectExploredLibrary)
    }
    transaction { } // ?

    var factory = ConnectionFactory()

    factory.host = "10.100.174.242"
    factory.username = "aaa"
    factory.password = "aaa"

    //TODO: refactor somehow

    val ERROR_TAG = "ERROR: "

    factory.exceptionHandler = object : ForgivingExceptionHandler() {

        override fun log(message: String?, e: Throwable?) {
            fileLogger!!.log(Level.SEVERE, ERROR_TAG + "Consumption failed with reason: $message")
            fileLogger!!.log(Level.INFO, ERROR_TAG + "${e?.localizedMessage}")
            fileLogger!!.log(Level.INFO, ERROR_TAG + e?.javaClass?.canonicalName)
        }

        override fun handleConsumerException(channel: Channel?, exception: Throwable?,
                                             consumer: Consumer?, consumerTag: String?, methodName: String?) {
            super.handleConsumerException(channel, exception, consumer, consumerTag, methodName)
            countMessages() // TODO: посмотреть, попадаем ли мы сюда с covered-исключениями

        }
    }


    //factory.setRequestedHeartbeat(5);
    //factory.connectionTimeout=5;
    // TODO
    //factory.shutdownTimeout = 10;

    val connection = factory.newConnection()

    channel = connection.createChannel()

    //connection.setHeartbeat(5);

    //TODO
    /* println(connection.heartbeat)

     println(factory.connectionTimeout)

     channel!!.addShutdownListener { cause: ShutdownSignalException? ->
         println(cause!!.message)
     }*/

    responseChannel = connection.createChannel()

    val args = HashMap<String, Any>()
    args.put("x-max-length", 200)
    channel!!.queueDeclare(TASKS_QUEUE_NAME, false, false, false, args)
    channel!!.queueDeclare(ACK_QUEUE_NAME, false, false, false, null)

    val consumer = GithubConsumer(connection, parsedArgs)
    channel!!.basicConsume(TASKS_QUEUE_NAME, false, consumer)
}

//TODO: structure (default message handling -> connection factory?)
private fun countMessages() {
    messageCounter++

    if (messageCounter % 100 == 0) {
        fileLogger!!.log(Level.INFO, "Acknowledgment has been sent (to approve consumption of 100 messages).")
        responseChannel!!.basicPublish("", ACK_QUEUE_NAME, null, "consumed".toByteArray())
    }
}