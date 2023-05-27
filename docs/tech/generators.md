# Data Generators

## Data Types

Below is a list of all supported data types for generating data:

| Data Type                 | Spark Data Type               | Options                                | Description                                               |
|---------------------------|-------------------------------|----------------------------------------|-----------------------------------------------------------|
| string                    | StringType                    | minLen, maxLen, expression, enableNull |                                                           |
| integer                   | IntegerType                   | min, minValue, max, maxValue           |                                                           |
| long                      | LongType                      | min, minValue, max, maxValue           |                                                           |
| short                     | ShortType                     | min, minValue, max, maxValue           |                                                           |
| decimal(precision, scale) | DecimalType(precision, scale) | min, minValue, max, maxValue           |                                                           |
| double                    | DoubleType                    | min, minValue, max, maxValue           |                                                           |
| float                     | FloatType                     | min, minValue, max, maxValue           |                                                           |
| date                      | DateType                      | min, max, enableNull                   |                                                           |
| timestamp                 | TimestampType                 | min, max, enableNull                   |                                                           |
| boolean                   | BooleanType                   |                                        |                                                           |
| binary                    | BinaryType                    | minLen, maxLen, enableNull             |                                                           |
| byte                      | ByteType                      |                                        |                                                           |
| array                     | ArrayType                     | listMinLen, listMaxLen                 |                                                           |
| _                         | StructType                    |                                        | Implicitly supported when a schema is defined for a field |

## Options

### All data types

Some options are available to use for all types of data generators. Below is the list along with example and
descriptions:

| Option          | Default | Example                 | Description                                                                                                                                                     |
|-----------------|---------|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| enableEdgeCases | false   | enableEdgeCases: "true" | Enable/disable generated data to contain edge cases based on the data type. For example, integer data type has edge cases of (Int.MaxValue, Int.MinValue and 0) |
| isUnique        | false   | isUnique: "true"        | Enable/disable generated data to be unique for that column. Errors will be thrown when it is unable to generate unique data                                     |
| seed            | <N/A>   | seed: "1"               | Defines the random seed for generating data for that particular column. It will override any seed defined at a global level                                     |

### String

| Option     | Default | Example                                                                                   | Description                                                                                                                                                                                                                                |
|------------|---------|-------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| minLen     | 1       | minLen: "2"                                                                               | Ensures that all generated strings have at least length `minLen`                                                                                                                                                                           |
| maxLen     | 10      | maxLen: "15"                                                                              | Ensures that all generated strings have at most length `maxLen`                                                                                                                                                                            |
| expression | <empty> | expression: "#{Name.name}"<br/> expression:"#{Address.city}/#{Demographic.maritalStatus}" | Will generate a string based on the faker expression provided. All possible faker expressions can be found [here](../../app/src/test/resources/datafaker/expressions.txt)<br/> Expression has to be in format `#{<faker expression name>}` |
| enableNull | false   | enableNull: "true"                                                                        | Enable/disbale null values being generated                                                                                                                                                                                                 |

### Numeric
For all the numeric data types, there are 4 options to choose from: min, minValue, max and maxValue.
Generally speaking, you only need to define one of min or minValue, similarly with max or maxValue.  
The reason why there are 2 options for each is because of when metadata is automatically gathered, we gather the statistics of the observed min and max values. Also, it will attempt to gather any restriction on the min or max value as defined by the data source (i.e. max value as per database type).

#### Integer/Long/Short/Decimal

| Option   | Default | Example        | Description                                                                                                                                                   |
|----------|---------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| minValue | 0       | minValue: "2"  | Ensures that all generated values are greater than or equal to `minValue`                                                                                     |
| min      | 0       | min: "2"       | Ensures that all generated values are greater than or equal to `min`. If `minValue` is defined, `minValue` will define the lowest possible generated value    |
| maxValue | 1000    | maxValue: "25" | Ensures that all generated values are less than or equal to `maxValue`                                                                                        |
| max      | 1000    | max: "25"      | Ensures that all generated values are less than or equal to `maxValue`. If `maxValue` is defined, `maxValue` will define the largest possible generated value |

#### Double/Float

| Option   | Default | Example          | Description                                                                                                                                                   |
|----------|---------|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| minValue | 0.0     | minValue: "2.1"  | Ensures that all generated values are greater than or equal to `minValue`                                                                                     |
| min      | 0.0     | min: "2.1"       | Ensures that all generated values are greater than or equal to `min`. If `minValue` is defined, `minValue` will define the lowest possible generated value    |
| maxValue | 1000.0  | maxValue: "25.9" | Ensures that all generated values are less than or equal to `maxValue`                                                                                        |
| max      | 1000.0  | max: "25.9"      | Ensures that all generated values are less than or equal to `maxValue`. If `maxValue` is defined, `maxValue` will define the largest possible generated value |

### Date

### Timestamp

### Binary

### List

