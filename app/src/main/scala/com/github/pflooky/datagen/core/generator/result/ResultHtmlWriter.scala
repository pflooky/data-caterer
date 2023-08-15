package com.github.pflooky.datagen.core.generator.result

import com.github.pflooky.datagen.core.model.Constants.HISTOGRAM
import com.github.pflooky.datagen.core.model.{DataSourceResult, DataSourceResultSummary, Generator, Plan, Step, StepResultSummary, TaskResultSummary}
import org.joda.time.DateTime

import scala.xml.{Node, NodeBuffer, NodeSeq}

class ResultHtmlWriter {

  private val css =
    "table.codegrid { font-family: monospace; font-size: 12px; width: auto!important; }" +
      "table.statementlist { width: auto!important; font-size: 13px; } " +
      "table.codegrid td { padding: 0!important; border: 0!important } " +
      "table td.linenumber { width: 40px!important; } " +
      "td { white-space:pre-line } " +
      ".table thead th { position: sticky; top: 0; z-index: 1; } "

  def index: Node = {
    <html>
      <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <title id='title'>Data Caterer</title>
      </head>
      <iframe name="navBar" class="navbar" src="navbar.html" width="10%" height="100%"></iframe>
      <iframe name="mainFrame" class="mainContainer" src="overview.html" width="89%" height="100%"></iframe>
    </html>
  }

  def overview(plan: Plan, stepResultSummary: List[StepResultSummary],
               taskResultSummary: List[TaskResultSummary], dataSourceResultSummary: List[DataSourceResultSummary]): Node = {
    <html>
      <head>
        <title>
          Overview - Data Caterer
        </title>{plugins}<style>
        {css}
      </style>
      </head>
      <body>
        <div>Generated at
          {DateTime.now()}
        </div>
        <h1>Summary</h1>{planSummary(plan, stepResultSummary, taskResultSummary, dataSourceResultSummary)}<h2>Task Summary</h2>{tasksSummary(taskResultSummary)}<h2>Step Summary</h2>{stepsSummary(stepResultSummary)}
      </body>
    </html>
  }

  def navBarDetails: Node = {
    <html>
      <head>
        <title>
          Overview - Data Caterer
        </title>{plugins}<style>
        {css}
      </style>
      </head>
      <body>
        <table class="tableFixHead table table-striped" style="font-size: 13px">
          <thead>
            <tr>
              <th>
                <a href="overview.html" target="mainFrame">Overview</a>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>
                <a href="plan.html" target="mainFrame">Plan</a>
              </td>
            </tr>
            <tr>
              <td>
                <a href="tasks.html" target="mainFrame">Task</a>
              </td>
            </tr>
            <tr>
              <td>
                <a href="steps.html" target="mainFrame">Step</a>
              </td>
            </tr>
            <tr>
              <td>
                <a href="data-sources.html" target="mainFrame">Data Source</a>
              </td>
            </tr>
          </tbody>
        </table>
      </body>
    </html>
  }

  def planSummary(plan: Plan, stepResultSummary: List[StepResultSummary],
                  taskResultSummary: List[TaskResultSummary], dataSourceResultSummary: List[DataSourceResultSummary]): Node = {
    val totalRecords = stepResultSummary.map(_.numRecords).sum
    val isSuccess = stepResultSummary.forall(_.isSuccess)
    <table class="tablesorter table table-striped" style="font-size: 13px">
      <thead>
        <tr>
          <th>Plan Name</th>
          <th>Num Records</th>
          <th>Success</th>
          <th>Tasks</th>
          <th>Steps</th>
          <th>Data Sources</th>
          <th>Foreign Keys</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>
            {plan.name}
          </td>
          <td>
            {totalRecords}
          </td>
          <td>
            {checkMark(isSuccess)}
          </td>
          <td>
            {taskResultSummary.size}
          </td>
          <td>
            {stepResultSummary.size}
          </td>
          <td>
            {dataSourceResultSummary.size}
          </td>
          <td>
            {plan.sinkOptions.map(_.foreignKeys).getOrElse(Map())}
          </td>
        </tr>
      </tbody>
    </table>
  }

  def planDetails(plan: Plan): Node = {
    <html>
    </html>
  }

  def tasksSummary(taskResultSummary: List[TaskResultSummary]): Node = {
    <table class="tablesorter table table-striped" style="font-size: 13px">
      <thead>
        <tr>
          <th>Name</th>
          <th>Num Records</th>
          <th>Success</th>
          <th>Steps</th>
        </tr>
      </thead>
      <tbody>
        {taskResultSummary.map(res => {
        val taskRef = s"tasks.html#${res.task.name}"
        <tr>
          <td>
            <a href={taskRef}>
              {res.task.name}
            </a>
          </td>
          <td>
            {res.numRecords}
          </td>
          <td>
            {checkMark(res.isSuccess)}
          </td>
          <td>
            {toStepLinks(res.task.steps)}
          </td>
        </tr>
      })}
      </tbody>
    </table>
  }

  def taskDetails(taskResultSummary: List[TaskResultSummary]): Node = {
    <html>
      <head>
        <title>
          Task Details - Data Caterer
        </title>{plugins}<style>
        {css}
      </style>
      </head>
      <body>
        <h1>Tasks</h1>
        <table class="tablesorter table table-striped" style="font-size: 13px">
          <thead>
            <tr>
              <th>Name</th>
              <th>Steps</th>
            </tr>
          </thead>
          <tbody>
            {taskResultSummary.map(res => {
            <tr id={res.task.name}>
              <td>
                {res.task.name}
              </td>
              <td>
                {toStepLinks(res.task.steps)}
              </td>
            </tr>
          })}
          </tbody>
        </table>
      </body>
    </html>
  }

  def stepsSummary(stepResultSummary: List[StepResultSummary]): Node = {
    <table class="tablesorter table table-striped" style="font-size: 13px">
      <thead>
        <tr>
          <th>Name</th>
          <th>Options</th>
          <th>Num Records</th>
          <th>Success</th>
          <th>Num Batches</th>
          <th>Time Taken (s)</th>
        </tr>
      </thead>
      <tbody>
        {stepResultSummary.map(res => {
        val stepLink = s"steps.html#${res.step.name}"
        <tr>
          <td>
            <a href={stepLink}>
              {res.step.name}
            </a>
          </td>
          <td>
            {optionsString(res)}
          </td>
          <td>
            {res.numRecords}
          </td>
          <td>
            {checkMark(res.isSuccess)}
          </td>
          <td>
            {res.dataSourceResults.map(_.batchNum).max}
          </td>
          <td>
            {res.dataSourceResults.map(_.sinkResult.durationInSeconds).sum}
          </td>
        </tr>
      })}
      </tbody>
    </table>
  }

  def stepDetails(stepResultSummary: List[StepResultSummary]): Node = {
    <html>
      <head>
        <title>
          Step Details - Data Caterer
        </title>{plugins}<style>
        {css}
      </style>
      </head>
      <body>
        <h1>Steps</h1>
        <table class="tablesorter table table-striped" style="font-size: 13px">
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Enabled</th>
              <th>Success</th>
              <th>Options</th>
              <th>Count</th>
              <th>Num Records</th>
              <th>Fields</th>
            </tr>
          </thead>
          <tbody>
            {stepResultSummary.map(res => {
            <tr id={res.step.name}>
              <td>
                {res.step.name}
              </td>
              <td>
                {res.step.`type`}
              </td>
              <td>
                {checkMark(res.step.enabled)}
              </td>
              <td>
                {checkMark(res.isSuccess)}
              </td>
              <td>
                {optionsString(res)}
              </td>
              <td>
                {res.step.count.numRecordsString}
              </td>
              <td>
                {res.numRecords}
              </td>
              <td>
                {fieldMetadata(res.step, res.dataSourceResults)}
              </td>
            </tr>
          })}
          </tbody>
        </table>
      </body>
    </html>
  }

  def fieldMetadata(step: Step, dataSourceResults: List[DataSourceResult]): Node = {
    val originalFields = step.schema.fields.getOrElse(List())
    val generatedFields = dataSourceResults.head.sinkResult.generatedMetadata
    val metadataMatch = originalFields.map(field => {
      val genField = generatedFields.filter(f => f.name == field.name).head
      val genMetadata = genField.generator.getOrElse(Generator()).options
      val originalMetadata = field.generator.getOrElse(Generator()).options
      val metadataCompare = (originalMetadata.keys ++ genMetadata.keys).filter(_ != HISTOGRAM).toList.distinct
        .map(key => s"$key: ${originalMetadata.getOrElse(key, "")} -> ${genMetadata.getOrElse(key, "")}")
      (field.name, metadataCompare)
    }).toMap
    <html>
      <body>
        <table class="tablesorter table table-striped" style="font-size: 13px">
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Nullable</th>
              <th>Generator Type</th>
              <th>Metadata</th>
              <th>Compare Generated Metadata</th>
            </tr>
          </thead>
          <tbody>
            {originalFields.map(field => {
            val generator = field.generator.getOrElse(Generator())
            <tr>
              <td>
                {field.name}
              </td>
              <td>
                {field.`type`.getOrElse("string")}
              </td>
              <td>
                {checkMark(field.nullable)}
              </td>
              <td>
                {generator.`type`}
              </td>
              <td>
                {generator.options.mkString("\n")}
              </td>
              <td>
                {metadataMatch(field.name).mkString("\n")}
              </td>
            </tr>
          })}
          </tbody>
        </table>
      </body>
    </html>
  }

  def dataSourceDetails(dataSourceResults: List[DataSourceResult]): Node = {
    <html>
    </html>
  }

  private def checkMark(isSuccess: Boolean): NodeSeq = if (isSuccess) xml.EntityRef("#9989") else xml.EntityRef("#10060")

  private def optionsString(res: StepResultSummary) = res.step.options.map(s => s"${s._1} -> ${s._2}").mkString("\n")

  private def toStepLinks(steps: List[Step]): Node = {
    {
      xml.Group(steps.map(step => {
        val stepLink = s"steps.html#${step.name}"
        <a href={stepLink}>
          {s"${step.name}"}
        </a>
      }))
    }
  }

  def plugins: NodeBuffer = {
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.20.1/css/theme.default.min.css" type="text/css"/>
      <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.20.1/js/jquery.tablesorter.min.js"></script>
        <link rel="stylesheet" href="https://netdna.bootstrapcdn.com/bootstrap/3.0.3/css/bootstrap.min.css" type="text/css"/>
      <script src="https://netdna.bootstrapcdn.com/bootstrap/3.0.3/js/bootstrap.min.js"></script>
      <script type="text/javascript">
        {xml.Unparsed(
        """$(document).ready(function() {$(".tablesorter").tablesorter();});"""
      )}
      </script>
  }
}
