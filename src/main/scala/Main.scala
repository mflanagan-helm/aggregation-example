import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import Json4SSerde.JsonSerde

case class CustomerProfile(id: Int, key: String, value: Double)
case class VoterRecord(id: Int, key: String, value: Double)
case class ResolvedProfile(cp_id: Int, vr_id: Int)

object Main {
  import Serdes._
  implicit val customerProfileSerde = new JsonSerde[CustomerProfile]
  implicit val voterRecordSerde = new JsonSerde[VoterRecord]
  implicit val resolvedProfileSerde = new JsonSerde[ResolvedProfile]
  implicit val voterRecordMapSerde = new JsonSerde[Map[Int, VoterRecord]]

  val CustomerProfileTopic = "CustomerProfile"
  val VoterRecordTopic = "VoterRecord"
  val ResolvedProfileTopic = "ResolvedProfile"

  def main(args: Array[String]) = {
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

    val resolvedProfileStream = customerProfileStream
      .selectKey((_, cp) => cp.key)
      .join(voterRecordsByKeyTable)(resolve)
      .selectKey((_, rp) => rp.cp_id)
    resolvedProfileStream.to(ResolvedProfileTopic)

    val topology = builder.build()
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

    val resolvedProfiles = resolvedProfileOutputTopic.readValuesToList().toArray()

    assert(resolvedProfiles(0) == ResolvedProfile(5,1))
    assert(resolvedProfiles(1) == ResolvedProfile(6,2))
    assert(resolvedProfiles(2) == ResolvedProfile(7,3))
    assert(resolvedProfiles(3) == ResolvedProfile(8,4))
  }
}