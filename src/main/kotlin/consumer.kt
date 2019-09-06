import com.ibm.icu.text.CharsetDetector
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Connection
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.xenomachina.argparser.InvalidArgumentException
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import java.io.*
import java.net.SocketTimeoutException
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

class GithubConsumer(private val connection: Connection, private val parsedArgs: MyArgs?) : DefaultConsumer(channel) {
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

            val downloadUrl = message;
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
                                it[GitProject.name] = projectName
                                it[GitProject.url] = downloadUrl.substringBeforeLast("/")
                                it[GitProject.charset] = realCharset
                                it[GitProject.lastDownloadDate] = System.currentTimeMillis()
                            }

                            val projectVersion = GitProjectVersion.insert {
                                it[GitProjectVersion.gitProjectId] = project[GitProjectVersion.id]
                                it[GitProjectVersion.versionHash] = downloadUrl.substringAfterLast("-")
                            }

                            GitProject.update({ GitProject.id eq project[GitProject.id] }) {
                                it[GitProject.downloadedProjectVersionId] = projectVersion[GitProject.id]
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