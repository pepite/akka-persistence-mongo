/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import com.typesafe.config.{Config, ConfigFactory}
import play.api.libs.iteratee._
import reactivemongo.api._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands._
import reactivemongo.api.indexes.{Index, IndexType}
import reactivemongo.bson._
import reactivemongo.core.nodeset.Authenticate

import scala.concurrent._
import scala.concurrent.duration.{DurationDouble, _}
import scala.util.{Failure, Success, Try}

object RxMongoPersistenceDriver {
  import MongoPersistenceDriver._

  def toWriteConcern(writeSafety: WriteSafety, wtimeout: Duration, fsync: Boolean): WriteConcern = (writeSafety, wtimeout.toMillis.toInt, fsync) match {
    case (Unacknowledged, wt, f) =>
      WriteConcern.Unacknowledged.copy(fsync = f, wtimeout = Option(wt))
    case (Acknowledged, wt, f) =>
      WriteConcern.Acknowledged.copy(fsync = f, wtimeout = Option(wt))
    case (Journaled, wt, _) =>
      WriteConcern.Journaled.copy(wtimeout = Option(wt))
    case (ReplicaAcknowledged, wt, f) =>
      WriteConcern.ReplicaAcknowledged(2, wt, !f)
  }
}

class RxMongoDriverProvider(actorSystem: ActorSystem) {
  val driver: MongoDriver = {
    val md = MongoDriver()
    actorSystem.registerOnTermination(driver.close())
    md
  }
}

class RxMongoDriver(system: ActorSystem, config: Config, driverProvider: RxMongoDriverProvider) extends MongoPersistenceDriver(system, config) {
  import RxMongoPersistenceDriver._

  val RxMongoSerializers: RxMongoSerializers = RxMongoSerializersExtension(system)

  // Collection type
  type C = Future[BSONCollection]

  type D = BSONDocument

  private def rxSettings = RxMongoDriverSettings(system.settings)
  private[mongodb] val driver = driverProvider.driver
  private[this] lazy val parsedMongoUri = MongoConnection.parseURI(mongoUri) match {
    case Success(parsed)    => parsed
    case Failure(throwable) => throw throwable
  }

  implicit val waitFor: FiniteDuration = 10.seconds

  private[this] lazy val unauthenticatedConnection: MongoConnection = wait {
    // create unauthenticated connection, there is no direct way to wait for authentication this way
    // plus prevent sending double authentication (initial authenticate and our explicit authenticate)
    driver.connection(parsedURI = parsedMongoUri.copy(authenticate = None))
      .database(name = dbName, failoverStrategy = failoverStrategy)(system.dispatcher)
      .map(_.connection)(system.dispatcher)
  }

  private[mongodb] lazy val connection: MongoConnection = {
    // now authenticate explicitly and wait for confirmation
    parsedMongoUri.authenticate.fold(unauthenticatedConnection) { auth =>
      waitForAuthentication(unauthenticatedConnection, auth)
    }
  }

  private[this] def waitForAuthentication(conn: MongoConnection, auth: Authenticate): MongoConnection = {
    wait(conn.authenticate(auth.db, auth.user, auth.password.getOrElse(""), failoverStrategy)(system.dispatcher))
    conn
  }
  private[this] def wait[T](awaitable: Awaitable[T])(implicit duration: Duration): T =
    Await.result(awaitable, duration)

  private[mongodb] def closeConnections(): Unit = {
    driver.close(5.seconds)
  }

  private[mongodb] def dbName: String = databaseName.getOrElse(parsedMongoUri.db.getOrElse(DEFAULT_DB_NAME))
  private[mongodb] def failoverStrategy: FailoverStrategy = {
    val rxMSettings = rxSettings
    FailoverStrategy(
      initialDelay = rxMSettings.InitialDelay,
      retries = rxMSettings.Retries,
      delayFactor = rxMSettings.GrowthFunction)
  }
  private[mongodb] def db = connection.database(name = dbName, failoverStrategy = failoverStrategy)(system.dispatcher)

  private[mongodb] override def collection(name: String) = db.map(_[BSONCollection](name))(system.dispatcher)

  private val NamespaceExistsErrorCode = 48
  private[mongodb] override def ensureCollection(name: String): Future[BSONCollection] = {
    implicit val ec: ExecutionContext = system.dispatcher
    for {
      coll <- collection(name)
      _ <- coll.create().recover { case CommandError.Code(NamespaceExistsErrorCode) => coll }
    } yield coll
  }

  private[mongodb] def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety, snapsWTimeout, snapsFsync)
  private[mongodb] def metadataWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)

  private[mongodb] override def ensureIndex(indexName: String, unique: Boolean, sparse: Boolean, keys: (String, Int)*)(implicit ec: ExecutionContext) = { collection =>
    val ky = keys.toSeq.map { case (f, o) => f -> (if (o > 0) IndexType.Ascending else IndexType.Descending) }
    collection.flatMap(c => c.indexesManager.ensure(Index(
      key = ky,
      background = true,
      unique = unique,
      sparse = sparse,
      name = Some(indexName))).map(_ => c))
  }

  override private[mongodb] def cappedCollection(name: String)(implicit ec: ExecutionContext) = {
    collection(name).flatMap { cc =>
      cc.stats().flatMap { s =>
        if (!s.capped) cc.convertToCapped(realtimeCollectionSize, None)
        else Future.successful(())
      }.recoverWith {
        case _ => {
          val c = cc.createCapped(realtimeCollectionSize, None)
          c
        }
      }.map(_ => cc)
    }
  }

  private[mongodb] def getCollections(collectionName: String)(implicit ec: ExecutionContext): Enumerator[BSONCollection] = {
    val fut = for {
      database  <- db
      names     <- database.collectionNames
      list      <- Future.sequence(names.filter(_.startsWith(collectionName)).map(collection))
    } yield Enumerator(list: _*)    
    Enumerator.flatten(fut)
  }

  private[mongodb] def getCollectionsAsFuture(collectionName: String)(implicit ec: ExecutionContext): Future[List[BSONCollection]] = {
    getAllCollectionsAsFuture(Option(_.startsWith(collectionName)))
  }

  private[mongodb] def getJournalCollections()(implicit ec: ExecutionContext) = getCollections(journalCollectionName)

  private[mongodb] def getAllCollectionsAsFuture(nameFilter: Option[String => Boolean])(implicit ec: ExecutionContext): Future[List[BSONCollection]] = {
    def excluded(name: String): Boolean =
      name == realtimeCollectionName ||
        name == metadataCollectionName ||
        name.startsWith("system.")

    def allPass(name: String): Boolean = true

    for {
      database  <- db
      names     <- database.collectionNames
      list      <- Future.sequence(names.filterNot(excluded).filter(nameFilter.getOrElse(allPass _)).map(collection))
    } yield list
  }

  private[mongodb] def journalCollectionsAsFuture(implicit ec: ExecutionContext) = getCollectionsAsFuture(journalCollectionName)
  
  private[mongodb] def getSnapshotCollections()(implicit ec: ExecutionContext) = getCollections(snapsCollectionName)


  override private[mongodb] def upgradeJournalIfNeeded: Unit = upgradeJournalIfNeeded("")

  implicit val materializer = ActorMaterializer()

  override private[mongodb] def upgradeJournalIfNeeded(persistenceId: String): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    import reactivemongo.akkastream.{ State, cursorProducer }

    val j = getJournal(persistenceId)
    val q = BSONDocument(JournallingFieldNames.TS -> BSONDocument("$exists" -> 0))

    realtime.map(_.drop(false))

    val future = j.flatMap { c =>

      val cnt = c.count(selector = Option(q))
      cnt.map { cnt =>
        println(s"Journal automatic upgrade found $cnt records needing upgrade")
        c.find(q).options(QueryOpts().noCursorTimeout.batchSize(20)).cursor[BSONDocument]().documentSource().mapAsync(20) (doc => {
          val id = doc.getAs[BSONObjectID]("_id").get
          val events = doc.getAs[BSONArray]("events").get
          // Update the doc with timestamps
          val time = id.time
          println(s"Journal automatic upgrade updating $id with TS -> $time")
          val evt = events.getAs[BSONDocument](0).get.merge(BSONDocument(JournallingFieldNames.TS -> time))
          val f = c.update(
            selector = BSONDocument("_id" -> id),
            update = BSONDocument(
              "$set" -> BSONDocument(JournallingFieldNames.TS -> time),
              "$set" -> BSONDocument("events" -> BSONArray(evt))
            )
          ).map { wr =>
            println(s"update ${wr.n} records")
            wr
          }
          f.onFailure { case e =>
              println(s"failed ${e} ")
              Future.failed(e)
            }
          f
        }).toMat(Sink.last[UpdateWriteResult])(Keep.right).run()
      }
    }
    Await.result(future, 10 minutes)
  }

  override private[mongodb] def upgradeSnapshotIfNeeded(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    import reactivemongo.akkastream.{ State, cursorProducer }

    val j = getSnapshotCollections()
    val q = BSONDocument(JournallingFieldNames.TS -> BSONDocument("$exists" -> 0))

    val future = j.map { c =>
      val cnt = c.count(selector = Option(q))
      cnt.map { cnt =>
        logger.info(s"Journal automatic upgrade found $cnt records needing upgrade")
        c.find(q, Some(BSONDocument("_id" -> 1))).options(QueryOpts().noCursorTimeout.batchSize(20)).cursor[BSONDocument]().documentSource().mapAsync(20) (doc => {
          val id = doc.getAs[BSONObjectID]("_id").get
          // Update the doc with timestamps
          val time = id.time
          c.update(BSONDocument("_id" -> id),
            update = BSONDocument(
              "$set" -> BSONDocument(JournallingFieldNames.TS -> time, "date" -> new BSONDateTime(time))
            )
          )
        }).toMat(Sink.last[UpdateWriteResult])(Keep.right).run()
      }
    }
    //Await.result(future, 10 minutes)
  }

  
}

class RxMongoPersistenceExtension(actorSystem: ActorSystem) extends MongoPersistenceExtension {

  val driverProvider: RxMongoDriverProvider = new RxMongoDriverProvider(actorSystem)

  override def configured(config: Config): Configured = Configured(config)

  case class Configured(config: Config) extends ConfiguredExtension {

    lazy val driver = new RxMongoDriver(actorSystem, config, driverProvider)

    override lazy val journaler: MongoPersistenceJournallingApi = new RxMongoJournaller(driver) with MongoPersistenceJournalMetrics {
      override def driverName = "rxmongo"
    }

    override lazy val snapshotter = new RxMongoSnapshotter(driver)
    override lazy val readJournal = new RxMongoReadJournaller(driver, ActorMaterializer()(actorSystem))

    if (driver.settings.JournalAutomaticUpgrade) {
      println("Journal automatic upgrade is enabled, executing upgrade process")
      driver.upgradeJournalIfNeeded()
      driver.upgradeSnapshotIfNeeded()
      println("Journal automatic upgrade process has completed")
    }

  }

}

object RxMongoDriverSettings {
  def apply(systemSettings: ActorSystem.Settings): RxMongoDriverSettings = {
    val fullName = s"${getClass.getPackage.getName}.rxmongo"
    val systemConfig = systemSettings.config
    systemConfig.checkValid(ConfigFactory.defaultReference(), fullName)
    new RxMongoDriverSettings(systemConfig.getConfig(fullName))
  }
}

class RxMongoDriverSettings(val config: Config) {

  config.checkValid(config, "failover")
  config.checkValid(config, "nbChannelsPerNode")

  private val failover = config.getConfig("failover")
  def InitialDelay: FiniteDuration = failover.getFiniteDuration("initialDelay")
  def Retries: Int = failover.getInt("retries")
  def Growth: String = failover.getString("growth")
  def ConstantGrowth: Boolean = Growth == "con"
  def LinearGrowth: Boolean = Growth == "lin"
  def ExponentialGrowth: Boolean = Growth == "exp"
  def Factor: Double = failover.getDouble("factor")

  def GrowthFunction: Int => Double = Growth match {
    case "con" => (_: Int) => Factor
    case "lin" => (i: Int) => i.toDouble
    case "exp" => (i: Int) => math.pow(i.toDouble, Factor)
  }
}
