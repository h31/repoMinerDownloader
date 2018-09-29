import com.ibm.icu.text.CharsetDetector
import com.ibm.icu.text.CharsetMatch
import com.rabbitmq.client.*
import com.rabbitmq.client.impl.ForgivingExceptionHandler
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.mainBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils.create
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import java.io.*
import java.net.SocketTimeoutException
import java.nio.charset.Charset
import java.util.*
import java.util.concurrent.*
import java.util.logging.FileHandler
import java.util.logging.Level
import java.util.logging.Logger
import java.util.logging.SimpleFormatter
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream


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

    val jdbc = "jdbc:mysql://127.0.0.1:3306/${parsedArgs!!.db}?serverTimezone=UTC"
    val driver = "com.mysql.cj.jdbc.Driver"

    val dbConnection = Database.connect(jdbc, user = parsedArgs!!.user, password = parsedArgs!!.password, driver = driver)

    transaction {
        create(GitProject, GitProjectVersion, ExploredLibrary, ExploredLibraryVersion, GitProjectExploredLibrary)
    }
    transaction { } // ?

    var factory = ConnectionFactory()

    factory.host = "localhost"

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

    val consumer = object : DefaultConsumer(channel) {
        @Throws(IOException::class)
        override fun handleDelivery(consumerTag: String, envelope: Envelope,
                                    properties: AMQP.BasicProperties, body: ByteArray) {
            val message = String(body, Charset.forName("UTF-8"))

            fileLogger!!.log(Level.INFO, "Received project link: $message, message number: $messageCounter")

            if (runRequest(message)) return

            countMessages()
        }

        private fun runRequest(message: String): Boolean {
            if (message == "stop") {
                channel!!.close()
                responseChannel!!.close()
                connection.close()
                fileHandler!!.close()
                return true
            } else {

                val downloadUrl = "https://api.github.com/repos/misiek/foo/zipball"//message;
                val projectName = downloadUrl.substringBeforeLast("/").substringAfterLast("/")

                val client = OkHttpClient.Builder().connectTimeout(10, TimeUnit.SECONDS)
                        .readTimeout(60 * 60 * 2, TimeUnit.SECONDS).build()

                val request = Request.Builder().url(downloadUrl).build()

                fileLogger!!.log(Level.INFO, "Start downloading...")

                lateinit var response: Response;

                response = client.newCall(request).execute()

                if (!response.isSuccessful) {

                    (response.body() as ResponseBody).close()

                    fileLogger!!.log(Level.SEVERE, "Request failed with reason: " + response)

                    return false
                } else {

                    val bytesInputStream = (response.body()!! as ResponseBody).byteStream()

                    val bytesOutputStream = ByteArrayOutputStream()

                    val byteBatchBuffer = ByteArray(16384)
                    var nRead: Int = bytesInputStream.read(byteBatchBuffer, 0, byteBatchBuffer.size)

                    while (nRead != -1) {

                        try {
                            bytesOutputStream.write(byteBatchBuffer, 0, nRead)                              // А здесь не надо оффсетить каждый раз на уже сдвинутое количество байт?
                            nRead = bytesInputStream.read(byteBatchBuffer, 0, byteBatchBuffer.size)
                        } catch (e: IOException) {

                            bytesInputStream.close()
                            bytesOutputStream.close()
                            (response.body() as ResponseBody).close()

                            if (e is SocketTimeoutException) {
                                fileLogger!!.log(Level.SEVERE,
                                        "This repository is too big to be obtained " +
                                                "during 2h and will be excluded because of it")
                                return false
                            } else {
                                fileLogger!!.log(Level.SEVERE, "Connection was aborted due to cause: ${e?.message}")
                                throw e
                            }
                        }
                    }

                    val data = bytesOutputStream.toByteArray()

                    bytesInputStream.close()
                    bytesOutputStream.close()

                    (response.body() as ResponseBody).close()

                    fileLogger!!.log(Level.INFO, "GitProject has been downloaded.")

                    val byteArrayInputStream = ByteArrayInputStream(data)
                    var possibleZipCharsets = detectPossibleZipCharsets(byteArrayInputStream)

                    var isJavaRepository = false
                    var realCharset: String? = null

                    for ((i, possibleZipCharset) in possibleZipCharsets.withIndex()) {

                        lateinit var zipInputStream: ZipInputStream
                        try {
                            zipInputStream = ZipInputStream(byteArrayInputStream, Charset.forName(possibleZipCharset))

                            var entry: ZipEntry? = zipInputStream.getNextEntry()

                            while (entry != null) {

                                if ((!entry.isDirectory) && (entry.name.endsWith(".java"))) {
                                    isJavaRepository = true
                                }
                                entry = zipInputStream.getNextEntry()
                            }

                            if (isJavaRepository) {                                 // Если Java-репозиторий и не вывалились с исключением
                                realCharset = possibleZipCharset
                                break;
                            }

                        } catch (e: Exception) {
                            if (!(e is InvalidArgumentException && e.message.equals("MALFORMED")))
                                throw e
                        } finally {
                            zipInputStream.close()
                        }
                    }

                    byteArrayInputStream.close()

                    var stored = false

                    if ((isJavaRepository) && (realCharset != null)) {

                        if (!checkAndCreateRepoFSStuffIfAbsent(true, parsedArgs!!.folder) ||
                                !checkAndCreateRepoFSStuffIfAbsent(false, parsedArgs!!.folder + "/" + projectName + ".zip")) {
                            fileLogger!!.log(Level.INFO, "GitProject's can't be stored on disk.")
                        } else {
                            realJavaReposCounter++

                            val fileOutputStream = FileOutputStream(parsedArgs!!.folder + "/" + projectName + ".zip")     //TODO: configure - whether save to File System or database
                            fileOutputStream.write(data)
                            fileOutputStream.close()

                            fileLogger!!.log(Level.INFO, "GitProject's files has been stored on disk.")
                            stored = true

                            transaction {

                                val project = GitProject.insert {
                                    it[name] = projectName
                                    it[url] = downloadUrl.substringBeforeLast("/")
                                    it[charset] = realCharset
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

                    when {
                        isJavaRepository && stored && (realCharset != null) ->
                            fileLogger!!.log(Level.INFO, "Java project correctly recognized: yes," +
                                    " number: $realJavaReposCounter")
                        isJavaRepository && (realCharset != null) ->
                            fileLogger!!.log(Level.INFO, "Java project correctly recognized , " +
                                    "but couldn't be stored.")
                        isJavaRepository ->
                            fileLogger!!.log(Level.INFO, "Java project wasn't correctly recognized (encoding). ")
                        else ->
                            fileLogger!!.log(Level.INFO, "Non-java project.")
                    }

                }
            }
            return false
        }

        private fun detectPossibleZipCharsets(zipInputStream: ByteArrayInputStream) =
                CharsetDetector().setText(zipInputStream).detectAll().map { it.name }.toList()


        private fun checkAndCreateRepoFSStuffIfAbsent(isDir: Boolean, path: String): Boolean {
            val file = File(path)
            if (!file.exists()) {
                when (isDir) {
                    true -> if (file.mkdir()) {
                        println("Directory ${parsedArgs!!.folder} is created!")
                        return true
                    } else {
                        println("Failed to create directory!")
                        return false
                    }
                    else -> if (file.createNewFile()) {
                        println("File ${file.toPath()} is created!")
                        return true
                    } else {
                        println("Failed to create file!")
                        return false
                    }
                }
            } else
                return true
        }
    }
    channel!!.basicConsume(TASKS_QUEUE_NAME, true, consumer)
}

//TODO: structure (default message handling -> connection factory?)
private fun countMessages() {
    messageCounter++

    if (messageCounter % 100 == 0) {
        fileLogger!!.log(Level.INFO, "Acknowledgment has been sent (to approve consumption of 100 messages).")
        responseChannel!!.basicPublish("", ACK_QUEUE_NAME, null, "consumed".toByteArray())
    }
}