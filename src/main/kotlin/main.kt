import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.mainBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.ResponseBody
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SchemaUtils.create
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.ByteArrayInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.nio.charset.Charset
import java.util.*
import java.util.logging.SimpleFormatter
import java.util.logging.FileHandler
import java.util.logging.Level
import java.util.logging.Logger
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream


/**
 * Created by Neverland on 18.01.2018.
 */

object GitProject : Table() {
    var id = integer("id").primaryKey().autoIncrement()
    var lastDownloadDate = long("last_download_date").nullable()
    var url = varchar("url", 250)
    var name = varchar("name", 250)
    var downloadedProjectVersionId=(integer("downloaded_project_version_id") ).nullable()
}

object GitProjectVersion : Table() {
    var id = integer("id").primaryKey().autoIncrement()
    var gitProjectId = integer("project_id") references GitProject.id
    var versionHash=varchar("version_hash", 250)
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

val TAG="repoMinerDownloader: "

val TASKS_QUEUE_NAME = "repositoryDownloadTasksQueue";
val ACK_QUEUE_NAME = "ackQueue";

var messageCounter=0

class MyArgs(parser: ArgParser) {

    val logger by parser.storing("logger's system path")
    val db by parser.storing("name of the database to store information about projects")
    val user by parser.storing("login for database to store information about projects")
    val password by parser.storing("password for database to store information about projects")

    val folder by parser.storing("system folder to store projcts' code")

}
var fileLogger: Logger? = null
var fileHandler: FileHandler? = null

var realJavaReposCounter=0

fun main(args: Array<String>) {

    var parsedArgs: MyArgs? = null

    mainBody {
        parsedArgs = ArgParser(args).parseInto(::MyArgs)
    }

    fileLogger = Logger.getLogger(TAG+"Log")


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

    val jdbc = "jdbc:mysql://127.0.0.1:3306/${parsedArgs!!.db}?serverTimezone=UTC"
    val driver = "com.mysql.cj.jdbc.Driver"

    val dbConnection = Database.connect(jdbc,user=parsedArgs!!.user, password = parsedArgs!!.password, driver = driver)

    transaction {
        create(GitProject, GitProjectVersion, ExploredLibrary, ExploredLibraryVersion, GitProjectExploredLibrary)
    }
    transaction {  }

    val factory = ConnectionFactory()
    factory.host = "localhost"
    factory.requestedHeartbeat = 0

    val connection = factory.newConnection()

    val channel = connection.createChannel()
    val responseChannel = connection.createChannel()

    val args = HashMap<String, Any>()
    args.put("x-max-length", 200)
    channel.queueDeclare(TASKS_QUEUE_NAME, false, false, false, args)
    channel.queueDeclare(ACK_QUEUE_NAME, false, false, false, null)

    val consumer = object : DefaultConsumer(channel) {
        @Throws(IOException::class)
        override fun handleDelivery(consumerTag: String, envelope: Envelope,
                                    properties: AMQP.BasicProperties, body: ByteArray) {
            val message = String(body, Charset.forName("UTF-8"))

            fileLogger!!.log(Level.INFO,"Received project link: $message, message number: $messageCounter")

            if (message == "stop") {
                channel.close()
                responseChannel.close()
                connection.close()
                fileHandler!!.close()
                return
            }else{

                val downloadUrl = message;
                val projectName = downloadUrl.substringBeforeLast("/").substringAfterLast("/")

                val client = OkHttpClient()
                val request = Request.Builder().url(downloadUrl).build()

                fileLogger!!.log(Level.INFO,"Start downloading...")
                val response = client.newCall(request).execute()

                if (!response.isSuccessful) {

                    (response.body() as ResponseBody).close()

                    fileLogger!!.log(Level.SEVERE,"Request failed with reason: "+ response)
                } else {

                    fileLogger!!.log(Level.INFO,"GitProject has been downloaded.")

                    val data=response.body()!!.bytes()

                    val zipInputStream= ZipInputStream(ByteArrayInputStream(data))

                    var entry: ZipEntry?=zipInputStream.getNextEntry()

                    var isJavaRepository=false

                    while (entry != null){

                        if ((!entry.isDirectory)&&(entry.name.endsWith(".java"))){
                            isJavaRepository=true
                            break
                        }
                        entry=zipInputStream.getNextEntry()
                    }

                    fileLogger!!.log(Level.INFO,"Java project: ${if (isJavaRepository) "yes, number: $realJavaReposCounter" else "no"}.")

                    if (isJavaRepository) {

                        realJavaReposCounter++

                        val fileOutputStream = FileOutputStream(parsedArgs!!.folder + "/" + projectName + ".zip")     //TODO: configure - whether save to File System or database
                        fileOutputStream.write(data)
                        fileOutputStream.close()

                        (response.body() as ResponseBody).close()

                        fileLogger!!.log(Level.INFO, "GitProject's files has been stored on disk.")

                        transaction {

                            val project = GitProject.insert {
                                it[name] = projectName
                                it[url] = downloadUrl.substringBeforeLast("/")
                                it[lastDownloadDate] = System.currentTimeMillis()
                            }

                            val projectVersion = GitProjectVersion.insert {
                                it[gitProjectId] = project[id]
                                it[versionHash] = downloadUrl.substringAfterLast("-")
                            }

                            GitProject.update({ GitProject.id eq project[GitProject.id] }) {
                                it[downloadedProjectVersionId] = projectVersion[id]
                            }

                        }
                        fileLogger!!.log(Level.INFO, "GitProject's information has been stored in database.")
                    }
                }
            }

            messageCounter++

            if(messageCounter%100==0) {
                fileLogger!!.log(Level.INFO,"Acknowledgment has been sent (to approve consumption of 100 messages).")
                responseChannel.basicPublish("", ACK_QUEUE_NAME, null, "consumed".toByteArray())
            }
        }
    }
    channel.basicConsume(TASKS_QUEUE_NAME, true, consumer)
}