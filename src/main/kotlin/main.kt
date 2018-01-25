import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.mainBody
import okhttp3.OkHttpClient
import okhttp3.Request
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SchemaUtils.create
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.FileOutputStream
import java.io.IOException
import java.nio.charset.Charset
import java.util.*

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

    val db by parser.storing("name of the database to store information about projects")
    val user by parser.storing("login for database to store information about projects")
    val password by parser.storing("password for database to store information about projects")

    val folder by parser.storing("system folder to store projcts' code")

}

fun main(args: Array<String>) {

    println(TAG+"I'm starting now...")

    var parsedArgs: MyArgs? = null

    mainBody {
        parsedArgs = ArgParser(args).parseInto(::MyArgs)
    }

    val jdbc = "jdbc:mysql://127.0.0.1:3306/${parsedArgs!!.db}?serverTimezone=UTC"
    val driver = "com.mysql.cj.jdbc.Driver"

    val dbConnection = Database.connect(jdbc,user=parsedArgs!!.user, password = parsedArgs!!.password, driver = driver)

    transaction {
        logger.addLogger(StdOutSqlLogger)
        create(GitProject, GitProjectVersion, ExploredLibrary, ExploredLibraryVersion, GitProjectExploredLibrary)
    }
    transaction {  }

    val factory = ConnectionFactory()
    factory.host = "localhost"
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

            println(TAG+"Received project link: $message, messge number: $messageCounter")

            if (message == "stop") {
                channel.close()
                responseChannel.close()
                connection.close()
                return
            }else{

                val downloadUrl = message;
                val projectName = downloadUrl.substringBeforeLast("/").substringAfterLast("/")

                val client = OkHttpClient()
                val request = Request.Builder().url(downloadUrl).build()

                println(TAG+"Start downloading...")
                val response = client.newCall(request).execute()

                if (!response.isSuccessful) {
                    println("Request failed with reason: "+ response)
                } else {

                    println(TAG+"GitProject has been downloaded.")

                    val fileOutputStream = FileOutputStream(parsedArgs!!.folder + "/" + projectName + ".zip")     //TODO: configure - whether save to File System or database
                    fileOutputStream.write(response.body()!!.bytes())
                    fileOutputStream.close()

                    println(TAG+"GitProject's files has been stored on disk.")

                    transaction {
                        logger.addLogger(StdOutSqlLogger)

                        val project= GitProject.insert {
                            it[name] = projectName
                            it[url] = downloadUrl.substringBeforeLast("/")
                            it[lastDownloadDate] = System.currentTimeMillis()
                        }

                        val projectVersion= GitProjectVersion.insert {
                            it[gitProjectId] = project[id]
                            it[versionHash] = downloadUrl.substringAfterLast("-")
                        }

                        GitProject.update({ GitProject.id eq project[GitProject.id]}){
                            it[downloadedProjectVersionId]=projectVersion[id]
                        }

                    }
                    println(TAG+"GitProject's information has been stored in database.")
                }
            }

            messageCounter++

            if(messageCounter%100==0) {
                println(TAG+"Acknowledgment has been sent (to approve consumption of 100 messages).")
                responseChannel.basicPublish("", ACK_QUEUE_NAME, null, "consumed".toByteArray())
            }
        }
    }
    channel.basicConsume(TASKS_QUEUE_NAME, true, consumer)
}