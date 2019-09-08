import com.ibm.icu.text.CharsetDetector
import com.rabbitmq.client.*
import com.rabbitmq.client.Connection
import com.xenomachina.argparser.InvalidArgumentException
import okhttp3.*
import org.eclipse.egit.github.core.RepositoryId
import org.eclipse.egit.github.core.service.ContentsService
import org.eclipse.egit.github.core.service.DataService
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

class GithubConsumer(private val connection: Connection, private val parsedArgs: MyArgs,
                     private val dataService: DataService) : DefaultConsumer(channel) {
    @Throws(IOException::class)
    override fun handleDelivery(consumerTag: String, envelope: Envelope,
                                properties: AMQP.BasicProperties, body: ByteArray) {
        val downloadUrl = String(body, Charset.forName("UTF-8"))
        val repo = RepositoryId.createFromUrl(downloadUrl)

        fileLogger!!.log(Level.INFO, "Received project link: $downloadUrl, message number: $messageCounter")

        runRequest(downloadUrl, repo.name)

        val deliveryTag = envelope.deliveryTag
        channel.basicAck(deliveryTag, false)
    }

    override fun handleShutdownSignal(consumerTag: String?, sig: ShutdownSignalException?) {
        channel!!.close()
        connection.close()
        fileHandler!!.close()
    }

    private fun isJava(repo: RepositoryId): Boolean {
        val tree = dataService.getTree(repo, "HEAD", true)
        if (tree.tree.none { it.path == "pom.xml" }) {
            println("No pom.xml, skipping")
            return false
        }
        if (tree.tree.none { it.path.endsWith(".java") }) {
            println("No Java files, skipping")
            return false
        }
        println("Java files detected, downloading...")
        return true
    }

    val client = OkHttpClient.Builder().connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(60 * 60 * 2, TimeUnit.SECONDS).build()

    private fun runRequest(downloadUrl: String, projectName: String) {
        fileLogger!!.log(Level.INFO, "GitProject has been downloaded.")

        if (!checkAndCreateRepoFSStuffIfAbsent(true, parsedArgs.folder) ||
                !checkAndCreateRepoFSStuffIfAbsent(false, "${parsedArgs.folder}/$projectName.tar.gz")) {
            fileLogger!!.log(Level.INFO, "GitProject's can't be stored on disk.")
            return
        }
        realJavaReposCounter++

        (getRepoArchive(downloadUrl) ?: return).use { responseBody ->
            FileOutputStream(parsedArgs.folder + "/" + projectName + ".zip").use { fileOutputStream ->     //TODO: configure - whether save to File System or database
                responseBody.byteStream().use { networkInputStream ->
                    networkInputStream.copyTo(fileOutputStream)
                }
            }
        }

        fileLogger!!.log(Level.INFO, "GitProject's files has been stored on disk.")

        transaction {

            val project = GitProject.insert {
                it[GitProject.name] = projectName
                it[GitProject.url] = downloadUrl.substringBeforeLast("/")
                it[GitProject.charset] = "UTF-8"
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

//    when
//    {
//        isJavaRepository && stored && (realCharset != null) ->
//        fileLogger!!.log(Level.INFO, "Java project correctly recognized: yes," +
//                " number: $realJavaReposCounter")
//        isJavaRepository && (realCharset != null) ->
//        fileLogger!!.log(Level.INFO, "Java project correctly recognized , " +
//                "but couldn't be stored.")
//        isJavaRepository ->
//        fileLogger!!.log(Level.INFO, "Java project wasn't correctly recognized (encoding). ")
//        else ->
//        fileLogger!!.log(Level.INFO, "Non-java project.")
//    }

}

private fun getRepoArchive(downloadUrl: String): ResponseBody? {
    val request = Request.Builder()
            .url("$downloadUrl/tarball")
            .header("Authorization", Credentials.basic(parsedArgs.user, parsedArgs.password))
            .build()

    fileLogger!!.log(Level.INFO, "Start downloading...")

    val response = client.newCall(request).execute()

    if (!response.isSuccessful) {

        response.body()?.close()

        fileLogger!!.log(Level.SEVERE, "Request failed with reason: $response")
    }

    return response.body()
}

private fun checkArchiveForJavaFiles(data: ByteArray?): Pair<Boolean, String?> {
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
    return Pair(isJavaRepository, realCharset)
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