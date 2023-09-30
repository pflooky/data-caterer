package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap
import com.github.pflooky.datacaterer.api.model.Constants.{METADATA_SOURCE_URL, OPEN_LINEAGE_DATASET, OPEN_LINEAGE_NAMESPACE}
import com.github.pflooky.datacaterer.api.model.{MarquezMetadataSource, MetadataSource}
import com.softwaremill.quicklens.ModifyPimp

case class MetadataSourceBuilder(metadataSource: MetadataSource = MarquezMetadataSource()) {
  def this() = this(MarquezMetadataSource())

  def marquez(url: String, namespace: String, optDataset: Option[String] = None, options: Map[String, String] = Map()): MetadataSourceBuilder = {
    val baseOptions = Map(
      METADATA_SOURCE_URL -> url,
      OPEN_LINEAGE_NAMESPACE -> namespace,
    ) ++ options
    val optionsWithDataset = optDataset.map(ds => baseOptions ++ Map(OPEN_LINEAGE_DATASET -> ds)).getOrElse(baseOptions)
    val marquezMetadataSource = MarquezMetadataSource(optionsWithDataset)
    this.modify(_.metadataSource).setTo(marquezMetadataSource)
  }

  def marquezJava(url: String, namespace: String, dataset: String, options: java.util.Map[String, String]): MetadataSourceBuilder =
    marquez(url, namespace, Some(dataset), toScalaMap(options))

  def marquez(url: String, namespace: String, dataset: String): MetadataSourceBuilder =
    marquez(url, namespace, Some(dataset), Map())

  def marquez(url: String, namespace: String): MetadataSourceBuilder =
    marquez(url, namespace, None, Map())
}
