import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import Json4SSerde.JsonSerde

case class CustomerProfile(id: Int, key: String, value: Double)
case class VoterRecord(id: Int, key: String, value: Double)
case class Matches(cp_id: Int, vr_ids: List[Int])
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
  implicit val matchesSerde = new JsonSerde[Matches]
  implicit val matchWithVrSerde = new JsonSerde[MatchWithVr]
  implicit val matchWithCpVrSerde = new JsonSerde[MatchWithCpVr]
  implicit val resolvedProfileSerde = new JsonSerde[ResolvedProfile]
  implicit val scoreSerde = new JsonSerde[Score]
  implicit val voterRecordMapSerde = new JsonSerde[Map[Int, VoterRecord]]
  implicit val optionalMinScoreSerde = new JsonSerde[Option[Score]]

  val CustomerProfileTopic = "CustomerProfile"
  val VoterRecordTopic = "VoterRecord"
  val MatchTopic = "Match"
  val MatchesTopic = "Matches"
  val MatchWithVrTopic = "MatchWithVr"
  val MatchWithCpVrTopic = "MatchWithCpVr"
  val ScoreTopic = "Score"
  val ResolvedProfileTopic = "ResolvedProfile"
  val ResolvedProfile2Topic = "ResolvedProfile2"

  def buildTopology() = {
    val builder = new StreamsBuilder

    val customerProfileStream = builder
      .stream[Int, CustomerProfile](CustomerProfileTopic)

    val customerProfileTable = customerProfileStream.toTable

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

    val matchWithCpVrStream = matchStream
      .selectKey((_, m) => m.vr_id)
      .join(voterRecordTable)((m: Match, vr: VoterRecord) => MatchWithVr(m.cp_id, vr))
      .selectKey((_, m) => m.cp_id)
      .join(customerProfileTable)((m: MatchWithVr, cp: CustomerProfile) => MatchWithCpVr(cp, m.vr))

    matchWithCpVrStream.to(MatchWithCpVrTopic)

    val scoreStream = matchWithCpVrStream
      .mapValues(m => Score(m.cp.id, m.vr.id, (m.cp.value - m.vr.value).abs))
    scoreStream.to(ScoreTopic)



    val resolvedProfileStream = customerProfileStream
      .selectKey((_, cp) => cp.key)
      .join(voterRecordsByKeyTable)(resolve)
      .selectKey((_, rp) => rp.cp_id)
    resolvedProfileStream.to(ResolvedProfileTopic)

    var resolvedProfile2Stream = scoreStream
      .groupByKey
      .aggregate(None: Option[Score])(
        (_, score, optMinScore) => {
          val minScore = optMinScore.getOrElse(score)
          if (score.score < minScore.score) {
            Some(score)
          }
          else {
            Some(minScore)
          }
        }
      )
      .toStream
      .mapValues(optMinScore => optMinScore.get)
      .mapValues(score => ResolvedProfile(score.cp_id, score.vr_id))
    resolvedProfile2Stream.to(ResolvedProfile2Topic)

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

    val matchWithCpVrOutputTopic = driver.createOutputTopic(
      MatchWithCpVrTopic,
      intSerde.deserializer(),
      matchWithCpVrSerde.deserializer()
    )

    val scoreOutputTopic = driver.createOutputTopic(
      ScoreTopic,
      intSerde.deserializer(),
      scoreSerde.deserializer()
    )

    val resolvedProfileOutputTopic = driver.createOutputTopic(
      ResolvedProfileTopic,
      intSerde.deserializer(),
      resolvedProfileSerde.deserializer())

    val resolvedProfile2OutputTopic = driver.createOutputTopic(
      ResolvedProfile2Topic,
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

    val matchWithCpVrs = matchWithCpVrOutputTopic.readKeyValuesToList().toArray()
    assert(matchWithCpVrs(0) == new KeyValue(5, MatchWithCpVr(CustomerProfile(5, "a", 0.5), VoterRecord(1, "a", 1.0))))
    assert(matchWithCpVrs(1) == new KeyValue(5, MatchWithCpVr(CustomerProfile(5, "a", 0.5), VoterRecord(3, "a", 3.0))))
    assert(matchWithCpVrs(2) == new KeyValue(6, MatchWithCpVr(CustomerProfile(6, "b", 1.0), VoterRecord(2, "b", 2.0))))
    assert(matchWithCpVrs(3) == new KeyValue(6, MatchWithCpVr(CustomerProfile(6, "b", 1.0), VoterRecord(4, "b", 4.0))))
    assert(matchWithCpVrs(4) == new KeyValue(7, MatchWithCpVr(CustomerProfile(7, "a", 2.5), VoterRecord(1, "a", 1.0))))
    assert(matchWithCpVrs(5) == new KeyValue(7, MatchWithCpVr(CustomerProfile(7, "a", 2.5), VoterRecord(3, "a", 3.0))))
    assert(matchWithCpVrs(6) == new KeyValue(8, MatchWithCpVr(CustomerProfile(8, "b", 3.5), VoterRecord(2, "b", 2.0))))
    assert(matchWithCpVrs(7) == new KeyValue(8, MatchWithCpVr(CustomerProfile(8, "b", 3.5), VoterRecord(4, "b", 4.0))))

    val scores = scoreOutputTopic.readKeyValuesToList().toArray()
    assert(scores(0) == new KeyValue(5, Score(5, 1, .5)))
    assert(scores(1) == new KeyValue(5, Score(5, 3, 2.5)))
    assert(scores(2) == new KeyValue(6, Score(6, 2, 1.0)))
    assert(scores(3) == new KeyValue(6, Score(6, 4, 3.0)))
    assert(scores(4) == new KeyValue(7, Score(7, 1, 1.5)))
    assert(scores(5) == new KeyValue(7, Score(7, 3, .5)))
    assert(scores(6) == new KeyValue(8, Score(8, 2, 1.5)))
    assert(scores(7) == new KeyValue(8, Score(8, 4, .5)))

    val resolvedProfiles2 = resolvedProfile2OutputTopic.readKeyValuesToList().toArray()
    assert(resolvedProfiles2(0) == new KeyValue(5, ResolvedProfile(5, 1)))
    assert(resolvedProfiles2(1) == new KeyValue(5, ResolvedProfile(5, 1)))
    assert(resolvedProfiles2(2) == new KeyValue(6, ResolvedProfile(6, 2)))
    assert(resolvedProfiles2(3) == new KeyValue(6, ResolvedProfile(6, 2)))
    assert(resolvedProfiles2(4) == new KeyValue(7, ResolvedProfile(7, 1)))
    assert(resolvedProfiles2(5) == new KeyValue(7, ResolvedProfile(7, 3)))
    assert(resolvedProfiles2(6) == new KeyValue(8, ResolvedProfile(8, 2)))
    assert(resolvedProfiles2(7) == new KeyValue(8, ResolvedProfile(8, 4)))

  }
}