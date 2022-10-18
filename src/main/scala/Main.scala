import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import Json4SSerde.JsonSerde

case class CustomerProfile(id: Int, key: String, value: Double)
case class VoterRecord(id: Int, key: String, value: Double)
case class CpMatches(cp: CustomerProfile, vr_ids: Set[Int])
case class CpMatch(cp: CustomerProfile, vr_id: Int)
case class VrMatches(vr: VoterRecord, cp_ids: Set[Int])
case class VrMatch(vr: VoterRecord, cp_id: Int)
case class Match(cp: CustomerProfile, vr: VoterRecord)
case class Score(cp_id: Int, vr_id: Int, score: Double)
case class ResolvedProfile(cp_id: Int, vr_id: Int)

object Main {
  import Serdes._
  implicit val customerProfileSerde = new JsonSerde[CustomerProfile]
  implicit val voterRecordSerde = new JsonSerde[VoterRecord]
  implicit val cpMatches = new JsonSerde[CpMatches]
  implicit val cpMatch = new JsonSerde[CpMatch]
  implicit val vrMatches = new JsonSerde[VrMatches]
  implicit val vrMatch = new JsonSerde[VrMatch]
  implicit val matchSerde = new JsonSerde[Match]
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
    val customerProfileIdsByKeyTable = customerProfileStream
      .selectKey((_, cp) => cp.key)
      .groupByKey
      .aggregate(Set(): Set[Int])(
        (_, cp, set) => set + cp.id,
      )

    val voterRecordStream = builder.stream[Int, VoterRecord](VoterRecordTopic)
    val voterRecordTable = voterRecordStream.toTable
    val voterRecordIdsByKeyTable = voterRecordStream
      .selectKey((_, vr) => vr.key)
      .groupByKey
      .aggregate(Set(): Set[Int])(
        (_, vr, set) => set + vr.id,
      )

    val cpMatches = customerProfileStream
      .selectKey((_, cp) => cp.key)
      .join(voterRecordIdsByKeyTable)((cp: CustomerProfile, vr_ids: Set[Int]) => CpMatches(cp, vr_ids) )
      .flatMapValues(v => v.vr_ids.map(vr_id => CpMatch(v.cp, vr_id)))
      .selectKey((_, m) => m.vr_id)
      .join(voterRecordTable)((m: CpMatch, vr: VoterRecord) => Match(m.cp, vr))

    val vrMatches = voterRecordStream
      .selectKey((_, vr) => vr.key)
      .join(voterRecordIdsByKeyTable)((vr: VoterRecord, cp_ids: Set[Int]) => VrMatches(vr, cp_ids) )
      .flatMapValues(v => v.cp_ids.map(cp_id => VrMatch(v.vr, cp_id)))
      .selectKey((_, m) => m.cp_id)
      .join(customerProfileTable)((m: VrMatch, cp: CustomerProfile) => Match(cp, m.vr))

    cpMatches
      .merge(vrMatches)
      .mapValues(m => Score(m.cp.id, m.vr.id, (m.cp.value - m.vr.value).abs))
      .selectKey((_, s) => s.cp_id)
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