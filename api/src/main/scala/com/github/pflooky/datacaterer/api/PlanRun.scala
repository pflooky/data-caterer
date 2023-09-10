package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.connection.{CassandraBuilder, ConnectionTaskBuilder, FileBuilder, HttpBuilder, JdbcBuilder, KafkaBuilder, SolaceBuilder}
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
              ): JdbcBuilder =
    ConnectionConfigWithTaskBuilder().jdbc(name, POSTGRES, url, username, password, options)

  def mysql(
             name: String,
             url: String = DEFAULT_MYSQL_URL,
             username: String = DEFAULT_MYSQL_USERNAME,
             password: String = DEFAULT_MYSQL_PASSWORD,
             options: Map[String, String] = Map()
           ): JdbcBuilder =
    ConnectionConfigWithTaskBuilder().jdbc(name, MYSQL, url, username, password, options)

  def cassandra(
                 name: String,
                 url: String = DEFAULT_CASSANDRA_URL,
                 username: String = DEFAULT_CASSANDRA_USERNAME,
                 password: String = DEFAULT_CASSANDRA_PASSWORD,
                 options: Map[String, String] = Map()
               ): CassandraBuilder =
    ConnectionConfigWithTaskBuilder().cassandra(name, url, username, password, options)

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

  def kafka(name: String, url: String = DEFAULT_KAFKA_URL, options: Map[String, String] = Map()): KafkaBuilder =
    ConnectionConfigWithTaskBuilder().kafka(name, url, options)

  def http(name: String, username: String = "", password: String = "", options: Map[String, String] = Map()): HttpBuilder =
    ConnectionConfigWithTaskBuilder().http(name, username, password, options)


  def execute(connectionTaskBuilder: ConnectionTaskBuilder, connectionTaskBuilders: ConnectionTaskBuilder*): Unit = {
    execute(configuration, connectionTaskBuilder, connectionTaskBuilders: _*)
  }

  def execute(baseConfiguration: DataCatererConfigurationBuilder, connectionTask: ConnectionTaskBuilder, connectionTasks: ConnectionTaskBuilder*): Unit = {
    val allConnectionTasks = connectionTask +: connectionTasks
    val connectionConfig = allConnectionTasks.map(x => {
      val connectionConfigWithTaskBuilder = x.connectionConfigWithTaskBuilder
      (connectionConfigWithTaskBuilder.dataSourceName, connectionConfigWithTaskBuilder.options)
    }).toMap

    val withConnectionConfig = baseConfiguration.connectionConfig(connectionConfig)
    val allTasks = allConnectionTasks.map(_.toTasksBuilder)

    execute(allTasks.toList, configuration = withConnectionConfig)
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
}

class BasePlanRun extends PlanRun
