import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import Json4SSerde.JsonSerde

case class CustomerProfile(id: Int, key: String, value: Double)
case class VoterRecord(id: Int, key: String, value: Double)
case class Matches(cp_id: Int, vr_ids: Set[Int])
case class Match(cp_id: Int, vr_id: Int)
case class MatchWithVr(cp_id: Int, vr: VoterRecord)
case class MatchWithCpVr(cp: CustomerProfile, vr: VoterRecord)
case class Score(cp_id: Int, vr_id: Int, score: Double)
case class ResolvedProfile(cp_id: Int, vr_id: Int)

object Main {
  import Serdes._
  implicit val customerProfileSerde = new JsonSerde[CustomerProfile]
  implicit val voterRecordSerde = new JsonSerde[VoterRecord]
  implicit val matchSerde = new JsonSerde[Match]
  implicit val matchWithVrSerde = new JsonSerde[MatchWithVr]
  implicit val matchWithCpVrSerde = new JsonSerde[MatchWithCpVr]
  implicit val resolvedProfileSerde = new JsonSerde[ResolvedProfile]
  implicit val scoreSerde = new JsonSerde[Score]
  implicit val voterRecordIdSetSerde = new JsonSerde[Set[Int]]
  implicit val optionalMinScoreSerde = new JsonSerde[Option[Score]]

  val CustomerProfileTopic = "CustomerProfile"
  val VoterRecordTopic = "VoterRecord"
  val ResolvedProfileTopic = "ResolvedProfile"

  def buildTopology() = {
    val builder = new StreamsBuilder

    val customerProfileStream = builder.stream[Int, CustomerProfile](CustomerProfileTopic)
    val customerProfileTable = customerProfileStream.toTable
    val voterRecordStream = builder.stream[Int, VoterRecord](VoterRecordTopic)
    val voterRecordTable = voterRecordStream.toTable
    val voterRecordIdsByKeyTable = voterRecordTable
      .groupBy((_, vr) => (vr.key, vr))
      .aggregate(Set(): Set[Int])(
        (_, vr, set) => set + vr.id,
        (_, vr, set) => set - vr.id
      )

    customerProfileStream
      .selectKey((_, cp) => cp.key)
      .join(voterRecordIdsByKeyTable)((cp: CustomerProfile, vr_ids: Set[Int]) => Matches(cp.id, vr_ids) )
      .flatMapValues(v => v.vr_ids.map(vr_id => Match(v.cp_id, vr_id)))
      .selectKey((_, m) => m.vr_id)
      .join(voterRecordTable)((m: Match, vr: VoterRecord) => MatchWithVr(m.cp_id, vr))
      .selectKey((_, m) => m.cp_id)
      .join(customerProfileTable)((m: MatchWithVr, cp: CustomerProfile) => MatchWithCpVr(cp, m.vr))
      .mapValues(m => Score(m.cp.id, m.vr.id, (m.cp.value - m.vr.value).abs))
      .groupByKey
      .aggregate(None: Option[Score])(
        (_, score, optMinScore) => {
          val minScore = optMinScore.getOrElse(score)
          if (score.score < minScore.score) Some(score) else Some(minScore)
        }
      )
      .toStream
      .mapValues(optMinScore => optMinScore.get)
      .mapValues(score => ResolvedProfile(score.cp_id, score.vr_id))
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
    )
    customerProfiles.foreach(cp => customerProfileInputTopic.pipeInput(cp.id, cp))

    val resolvedProfiles = resolvedProfileOutputTopic.readKeyValuesToList().toArray()
    assert(resolvedProfiles(0) == new KeyValue(5, ResolvedProfile(5, 1)))
    assert(resolvedProfiles(1) == new KeyValue(5, ResolvedProfile(5, 1)))
    assert(resolvedProfiles(2) == new KeyValue(6, ResolvedProfile(6, 2)))
    assert(resolvedProfiles(3) == new KeyValue(6, ResolvedProfile(6, 2)))
    assert(resolvedProfiles(4) == new KeyValue(7, ResolvedProfile(7, 1)))
    assert(resolvedProfiles(5) == new KeyValue(7, ResolvedProfile(7, 3)))
    assert(resolvedProfiles(6) == new KeyValue(8, ResolvedProfile(8, 2)))
    assert(resolvedProfiles(7) == new KeyValue(8, ResolvedProfile(8, 4)))

  }
}