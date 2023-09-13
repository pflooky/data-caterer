package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.connection.{CassandraBuilder, ConnectionTaskBuilder, FileBuilder, HttpBuilder, KafkaBuilder, MySqlBuilder, PostgresBuilder, SolaceBuilder}
import com.github.pflooky.datacaterer.api.model.Constants._
import com.github.pflooky.datacaterer.api.model.{DataCatererConfiguration, ForeignKeyRelation, Plan, Task, ValidationConfiguration}


trait PlanRun {
  var _plan: Plan = Plan()
  var _tasks: List[Task] = List()
  var _configuration: DataCatererConfiguration = DataCatererConfiguration()
  var _validations: List[ValidationConfiguration] = List()

  def plan: PlanBuilder = PlanBuilder()

  def taskSummary: TaskSummaryBuilder = TaskSummaryBuilder()

  def tasks: TasksBuilder = TasksBuilder()

  def task: TaskBuilder = TaskBuilder()

  def step: StepBuilder = StepBuilder()

  def schema: SchemaBuilder = SchemaBuilder()

  def field: FieldBuilder = FieldBuilder()

  def generator: GeneratorBuilder = GeneratorBuilder()

  def count: CountBuilder = CountBuilder()

  def configuration: DataCatererConfigurationBuilder = DataCatererConfigurationBuilder()

  def validation: ValidationBuilder = ValidationBuilder()

  def dataSourceValidation: DataSourceValidationBuilder = DataSourceValidationBuilder()

  def validationConfig: ValidationConfigurationBuilder = ValidationConfigurationBuilder()

  def foreignField(dataSource: String, step: String, column: String): ForeignKeyRelation = ForeignKeyRelation(dataSource, step, column)

  def csv(name: String, path: String, options: Map[String, String] = Map()): FileBuilder =
    ConnectionConfigWithTaskBuilder().file(name, CSV, path, options)

  def json(name: String, path: String, options: Map[String, String] = Map()): FileBuilder =
    ConnectionConfigWithTaskBuilder().file(name, JSON, path, options)

  def orc(name: String, path: String, options: Map[String, String] = Map()): FileBuilder =
    ConnectionConfigWithTaskBuilder().file(name, ORC, path, options)

  def parquet(name: String, path: String, options: Map[String, String] = Map()): FileBuilder =
    ConnectionConfigWithTaskBuilder().file(name, PARQUET, path, options)

  def postgres(
                name: String,
                url: String = DEFAULT_POSTGRES_URL,
                username: String = DEFAULT_POSTGRES_USERNAME,
                password: String = DEFAULT_POSTGRES_PASSWORD,
                options: Map[String, String] = Map()
              ): PostgresBuilder =
    ConnectionConfigWithTaskBuilder().postgres(name, url, username, password, options)

  def postgres(connectionTaskBuilder: ConnectionTaskBuilder[PostgresBuilder]): PostgresBuilder =
    PostgresBuilder().fromBaseConfig(connectionTaskBuilder)

  def mysql(
             name: String,
             url: String = DEFAULT_MYSQL_URL,
             username: String = DEFAULT_MYSQL_USERNAME,
             password: String = DEFAULT_MYSQL_PASSWORD,
             options: Map[String, String] = Map()
           ): MySqlBuilder =
    ConnectionConfigWithTaskBuilder().mySql(name, url, username, password, options)

  def mysql(connectionTaskBuilder: ConnectionTaskBuilder[MySqlBuilder]): MySqlBuilder =
    MySqlBuilder().fromBaseConfig(connectionTaskBuilder)

  def cassandra(
                 name: String,
                 url: String = DEFAULT_CASSANDRA_URL,
                 username: String = DEFAULT_CASSANDRA_USERNAME,
                 password: String = DEFAULT_CASSANDRA_PASSWORD,
                 options: Map[String, String] = Map()
               ): CassandraBuilder =
    ConnectionConfigWithTaskBuilder().cassandra(name, url, username, password, options)

  def cassandra(connectionTaskBuilder: ConnectionTaskBuilder[CassandraBuilder]): CassandraBuilder =
    CassandraBuilder().fromBaseConfig(connectionTaskBuilder)

  def solace(
              name: String,
              url: String = DEFAULT_SOLACE_URL,
              username: String = DEFAULT_SOLACE_USERNAME,
              password: String = DEFAULT_SOLACE_PASSWORD,
              vpnName: String = DEFAULT_SOLACE_VPN_NAME,
              connectionFactory: String = DEFAULT_SOLACE_CONNECTION_FACTORY,
              initialContextFactory: String = DEFAULT_SOLACE_INITIAL_CONTEXT_FACTORY,
              options: Map[String, String] = Map()
            ): SolaceBuilder =
    ConnectionConfigWithTaskBuilder().solace(name, url, username, password, vpnName, connectionFactory, initialContextFactory, options)

  def solace(connectionTaskBuilder: ConnectionTaskBuilder[SolaceBuilder]): SolaceBuilder =
    SolaceBuilder().fromBaseConfig(connectionTaskBuilder)

  def kafka(name: String, url: String = DEFAULT_KAFKA_URL, options: Map[String, String] = Map()): KafkaBuilder =
    ConnectionConfigWithTaskBuilder().kafka(name, url, options)

  def kafka(connectionTaskBuilder: ConnectionTaskBuilder[KafkaBuilder]): KafkaBuilder =
    KafkaBuilder().fromBaseConfig(connectionTaskBuilder)

  def http(name: String, username: String = "", password: String = "", options: Map[String, String] = Map()): HttpBuilder =
    ConnectionConfigWithTaskBuilder().http(name, username, password, options)

  def http(connectionTaskBuilder: ConnectionTaskBuilder[HttpBuilder]): HttpBuilder =
    HttpBuilder().fromBaseConfig(connectionTaskBuilder)


  def execute(connectionTaskBuilder: ConnectionTaskBuilder[_], connectionTaskBuilders: ConnectionTaskBuilder[_]*): Unit = {
    execute(configuration, connectionTaskBuilder, connectionTaskBuilders: _*)
  }

  def execute(
               baseConfiguration: DataCatererConfigurationBuilder,
               connectionTaskBuilder: ConnectionTaskBuilder[_],
               connectionTaskBuilders: ConnectionTaskBuilder[_]*
             ): Unit = {
    execute(plan, baseConfiguration, List(), connectionTaskBuilder, connectionTaskBuilders: _*)
  }

  def execute(
               planBuilder: PlanBuilder,
               baseConfiguration: DataCatererConfigurationBuilder,
               connectionTaskBuilder: ConnectionTaskBuilder[_],
               connectionTaskBuilders: ConnectionTaskBuilder[_]*
             ): Unit = {
    execute(planBuilder, baseConfiguration, List(), connectionTaskBuilder, connectionTaskBuilders: _*)
  }

  def execute(
               planBuilder: PlanBuilder,
               baseConfiguration: DataCatererConfigurationBuilder,
               validations: List[ValidationConfigurationBuilder],
               connectionTask: ConnectionTaskBuilder[_],
               connectionTasks: ConnectionTaskBuilder[_]*
             ): Unit = {
    val allConnectionTasks = connectionTask +: connectionTasks
    val connectionConfig = allConnectionTasks.map(x => {
      val connectionConfigWithTaskBuilder = x.connectionConfigWithTaskBuilder
      (connectionConfigWithTaskBuilder.dataSourceName, connectionConfigWithTaskBuilder.options)
    }).toMap
    val withConnectionConfig = baseConfiguration.connectionConfig(connectionConfig)
    val allValidations = validations ++ getValidations(allConnectionTasks)
    val allTasks = allConnectionTasks.map(_.toTasksBuilder).filter(_.isDefined).map(_.get).toList

    execute(allTasks, planBuilder, withConnectionConfig, allValidations)
  }

  def execute(tasks: TasksBuilder): Unit = execute(List(tasks))

  def execute(planBuilder: PlanBuilder, configuration: DataCatererConfigurationBuilder): Unit = {
    execute(planBuilder.tasks, planBuilder, configuration)
  }

  def execute(
               tasks: List[TasksBuilder] = List(),
               plan: PlanBuilder = PlanBuilder(),
               configuration: DataCatererConfigurationBuilder = DataCatererConfigurationBuilder(),
               validations: List[ValidationConfigurationBuilder] = List()
             ): Unit = {
    val taskToDataSource = tasks.flatMap(x => x.tasks.map(t => (t.name, x.dataSourceName, t)))
    val planWithTaskToDataSource = plan.taskSummaries(taskToDataSource.map(t => taskSummary.name(t._1).dataSource(t._2)): _*)

    _plan = planWithTaskToDataSource.plan
    _tasks = taskToDataSource.map(_._3)
    _configuration = configuration.build
    _validations = validations.map(_.validationConfiguration)
  }

  private def getValidations(allConnectionTasks: Seq[ConnectionTaskBuilder[_]]) = {
    allConnectionTasks.map(x => {
      val dataSource = x.connectionConfigWithTaskBuilder.dataSourceName
      val options = x.connectionConfigWithTaskBuilder.options
      val stepValidation = x.step.flatMap(_.optValidation).getOrElse(DataSourceValidationBuilder()).options(options)

      (dataSource, validationConfig.addDataSourceValidation(dataSource, stepValidation))
    })
      .groupBy(_._1)
      .map(x => {
        val dataSource = x._1
        val otherValidations = x._2.tail.flatMap(_._2.validationConfiguration.dataSources(dataSource).validations)
        if (otherValidations.nonEmpty) {
          x._2.head._2.addValidations(dataSource, otherValidations)
        } else {
          x._2.head._2
        }
      })
      .filter(vc => vc.validationConfiguration.dataSources.exists(ds => ds._2.validations.nonEmpty))
  }
}

class BasePlanRun extends PlanRun
