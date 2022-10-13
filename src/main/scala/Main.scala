import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import Json4SSerde.JsonSerde

case class CustomerProfile(id: Int, key: String, value: Double)
case class VoterRecord(id: Int, key: String, value: Double)
case class Match(cp_id: Int, vr_id: Int)
case class Matches(cp_id: Int, vr_ids: List[Int])
case class ResolvedProfile(cp_id: Int, vr_id: Int)

object Main {
  import Serdes._
  implicit val customerProfileSerde = new JsonSerde[CustomerProfile]
  implicit val voterRecordSerde = new JsonSerde[VoterRecord]
  implicit val matchSerde = new JsonSerde[Match]
  implicit val matchesSerde = new JsonSerde[Matches]
  implicit val resolvedProfileSerde = new JsonSerde[ResolvedProfile]
  implicit val voterRecordMapSerde = new JsonSerde[Map[Int, VoterRecord]]

  val CustomerProfileTopic = "CustomerProfile"
  val VoterRecordTopic = "VoterRecord"
  val MatchTopic = "Match"
  val MatchesTopic = "Matches"
  val ResolvedProfileTopic = "ResolvedProfile"

  def buildTopology() = {
    val builder = new StreamsBuilder

    val customerProfileStream = builder
      .stream[Int, CustomerProfile](CustomerProfileTopic)

    val voterRecordTable = builder
      .table[Int, VoterRecord](VoterRecordTopic)

    val voterRecordsByKeyTable = voterRecordTable
      .groupBy((_, vr) => (vr.key, vr))
      .aggregate(Map(): Map[Int, VoterRecord])(
        (_, vr, map) => map + (vr.id -> vr),
        (_, vr, map) => map - vr.id
      )

    def resolve(cp: CustomerProfile, vrs: Map[Int, VoterRecord]): ResolvedProfile = {
      val vrsList = vrs.values.toList
      val scores = vrsList.map(vr => (cp.value - vr.value).abs)
      val vrMatchIndex = scores.zipWithIndex.min._2
      val vrMatch = vrsList(vrMatchIndex)
      ResolvedProfile(cp.id, vrMatch.id)
    }

    def joinMatches(cp: CustomerProfile, vrs: Map[Int, VoterRecord]): Matches = {
      val vr_ids = vrs.keys.toList
      Matches(cp.id, vr_ids)
    }

    val matchesStream = customerProfileStream
      .selectKey((_, cp) => cp.key)
      .join(voterRecordsByKeyTable)(joinMatches)
      .selectKey((_, rp) => rp.cp_id)
    matchesStream.to(MatchesTopic)

    val matchStream = matchesStream
      .flatMapValues(v => v.vr_ids.map(vr_id => Match(v.cp_id, vr_id)))
    matchStream.to(MatchTopic)

    customerProfileStream
      .selectKey((_, cp) => cp.key)
      .join(voterRecordsByKeyTable)(resolve)
      .selectKey((_, rp) => rp.cp_id)
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

    val matchesOutputTopic = driver.createOutputTopic(
      MatchesTopic,
      intSerde.deserializer(),
      matchesSerde.deserializer()
    )

    val matchOutputTopic = driver.createOutputTopic(
      MatchTopic,
      intSerde.deserializer(),
      matchSerde.deserializer()
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

    val resolvedProfiles = resolvedProfileOutputTopic.readValuesToList().toArray()

    assert(resolvedProfiles(0) == ResolvedProfile(5, 1))
    assert(resolvedProfiles(1) == ResolvedProfile(6, 2))
    assert(resolvedProfiles(2) == ResolvedProfile(7, 3))
    assert(resolvedProfiles(3) == ResolvedProfile(8, 4))

    val matches = matchesOutputTopic.readValuesToList().toArray()

    assert(matches(0) == Matches(5, List(1, 3)))
    assert(matches(1) == Matches(6, List(2, 4)))
    assert(matches(2) == Matches(7, List(1, 3)))
    assert(matches(3) == Matches(8, List(2, 4)))

    val matchList = matchOutputTopic.readValuesToList().toArray()
    assert(matchList(0) == Match(5, 1))
    assert(matchList(1) == Match(5, 3))
    assert(matchList(2) == Match(6, 2))
    assert(matchList(3) == Match(6, 4))
    assert(matchList(4) == Match(7, 1))
    assert(matchList(5) == Match(7, 3))
    assert(matchList(6) == Match(8, 2))
    assert(matchList(7) == Match(8, 4))
  }
}