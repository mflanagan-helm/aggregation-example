import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import Json4SSerde.JsonSerde

case class CustomerProfile(id: Int, key: String, value: Double)
case class VoterRecord(id: Int, key: String, value: Double)
case class Score(cp_id: Int, vr_id: Int, score: Double)
case class ResolvedProfile(cp_id: Int, vr_id: Int)

object Main {
  import Serdes._
  implicit val customerProfileSerde = new JsonSerde[CustomerProfile]
  implicit val voterRecordSerde = new JsonSerde[VoterRecord]
  implicit val voterRecordIdSetSerde = new JsonSerde[Set[Int]]
  implicit val scoreSerde = new JsonSerde[Score]
  implicit val optionalMinScoreSerde = new JsonSerde[Option[Score]]
  implicit val resolvedProfileSerde = new JsonSerde[Option[ResolvedProfile]]

  val CustomerProfileTopic = "CustomerProfile"
  val VoterRecordTopic = "VoterRecord"
  val ResolvedProfileTopic = "ResolvedProfile"

  def buildTopology() = {
    val builder = new StreamsBuilder
    val customerProfileStream = builder.stream[Int, CustomerProfile](CustomerProfileTopic)
    val voterRecordStream = builder.stream[Int, VoterRecord](VoterRecordTopic)
    val voterRecordTable = voterRecordStream.toTable
    val voterRecordIdsByKeyTable = voterRecordStream
      .groupBy((_, v) => v.key)
      .aggregate(Set(): Set[Int])(
        (_, vr, set) => set + vr.id
      )


    val resolvedProfiles = customerProfileStream
      .selectKey((_, cp) => cp.key)
      .join(voterRecordIdsByKeyTable)((cp, vr_ids) => (cp, vr_ids) )
      .flatMap((_, v) => v._2.map(vr_id => (vr_id, v._1)))
      .join(voterRecordTable)((cp, vr) => Score(cp.id, vr.id, (cp.value - vr.value).abs))
      .groupBy((_, s) => s.cp_id)
      .aggregate(None: Option[Score])(
        (_, score, optMinScore) => {
          val minScore = optMinScore.getOrElse(score)
          if (score.score < minScore.score) Some(score) else Some(minScore)
        }
      )
      .toStream
      .mapValues(optMinScore => optMinScore.get)
      .mapValues(score => Some(ResolvedProfile(score.cp_id, score.vr_id)) : Option[ResolvedProfile])

    val resolvedProfilesForMissingVoterRecord = customerProfileStream
      .selectKey((_, cp) => cp.key)
      .leftJoin(voterRecordIdsByKeyTable)((cp, vr_ids) => (cp, vr_ids == null))
      .filter((_, v) => v._2)
      .selectKey((cp, v) => v._1.id)
      .mapValues(v => None: Option[ResolvedProfile])

    resolvedProfiles
      .merge(resolvedProfilesForMissingVoterRecord)
      .to(ResolvedProfileTopic)

    builder.build()
  }

  def main(args: Array[String]) = {
    val topology = Main.buildTopology()
    val driver = new TopologyTestDriver(topology)

    val customerProfileInputTopic = driver.createInputTopic(
      CustomerProfileTopic,
      intSerde.serializer(),
      customerProfileSerde.serializer()
    )

    val voterRecordInputTopic = driver.createInputTopic(
      VoterRecordTopic,
      intSerde.serializer(),
      voterRecordSerde.serializer()
    )

    val resolvedProfileOutputTopic = driver.createOutputTopic(
      ResolvedProfileTopic,
      intSerde.deserializer(),
      resolvedProfileSerde.deserializer())

    val voterRecords = List(
      VoterRecord(1, "a", 1.0),
      VoterRecord(2, "b", 2.0),
      VoterRecord(3, "a", 3.0),
      VoterRecord(4, "b", 4.0),
    )
    voterRecords.foreach(vr => voterRecordInputTopic.pipeInput(vr.id, vr))

    val customerProfiles = List(
      CustomerProfile(5, "a", 0.5),
      CustomerProfile(6, "b", 1.0),
      CustomerProfile(7, "a", 2.5),
      CustomerProfile(8, "b", 3.5),
      CustomerProfile(9, "c", 1.5)
    )
    customerProfiles.foreach(cp => customerProfileInputTopic.pipeInput(cp.id, cp))

    val resolvedProfiles = resolvedProfileOutputTopic.readKeyValuesToList().toArray()
    assert(resolvedProfiles(0) == new KeyValue(5, Some(ResolvedProfile(5, 1))))
    assert(resolvedProfiles(1) == new KeyValue(5, Some(ResolvedProfile(5, 1))))
    assert(resolvedProfiles(2) == new KeyValue(6, Some(ResolvedProfile(6, 2))))
    assert(resolvedProfiles(3) == new KeyValue(6, Some(ResolvedProfile(6, 2))))
    assert(resolvedProfiles(4) == new KeyValue(7, Some(ResolvedProfile(7, 1))))
    assert(resolvedProfiles(5) == new KeyValue(7, Some(ResolvedProfile(7, 3))))
    assert(resolvedProfiles(6) == new KeyValue(8, Some(ResolvedProfile(8, 2))))
    assert(resolvedProfiles(7) == new KeyValue(8, Some(ResolvedProfile(8, 4))))
    assert(resolvedProfiles(8) == new KeyValue(9, None))
  }
}