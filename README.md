# Excel Connector

Excel Connector is a connector implementation for the Universal Data Connectors framework that allows you to read and write Excel files (.xlsx format) with support for streaming operations.

## Features

- **Read Excel files** - Read data from Excel files with flexible configuration options
- **Write Excel files** - Write data to Excel files with automatic formatting
- **Streaming support** - Both streaming source and sink capabilities for large files
- **Flexible input/output** - Support for file paths, input streams, and output streams
- **Column filtering** - Select specific columns or column ranges
- **Row filtering** - Skip empty rows, set start row, and limit records
- **Sheet selection** - Select sheets by name or index
- **Header handling** - Automatic header detection and optional header row in output

## Installation

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.dataconnector</groupId>
    <artifactId>connector-excel</artifactId>
    <version>0.0.1</version>
</dependency>
```

### Build from Source

```bash
mvn clean install
```

## Configuration

### Required Configuration

At least one of the following is required:

- `file_path` (String) - Path to the Excel file (supports classpath resources)
- `input_stream` (InputStream) - Input stream for reading (read operations)
- `output_stream` (OutputStream) - Output stream for writing (write operations)

### Optional Configuration

#### Reading Configuration

| Configuration Key | Type | Default | Description |
|------------------|------|---------|-------------|
| `sheet_name` | String | First sheet | Name of the sheet to read from |
| `sheet_index` | Integer | 0 | Index of the sheet to read from (0-based) |
| `start_row` | Integer | 0 | Row index to start reading from (0-based) |
| `limit` | Integer | -1 | Maximum number of records to read (-1 for all) |
| `skip_empty_rows` | Boolean | true | Whether to skip empty rows |
| `use_first_row_as_headers` | Boolean | true | Whether to use the first row as column headers |
| `columns` | String/List | null | Column indices to include (e.g., "0,2-5,10" or [0,2,3,4,5,10]) |

#### Writing Configuration

| Configuration Key | Type | Default | Description |
|------------------|------|---------|-------------|
| `file_path` | String | Required* | Path to the output Excel file |
| `output_stream` | OutputStream | Required* | Output stream for writing |
| `sheet_name` | String | "Sheet1" | Name of the sheet to create |
| `use_first_row_as_headers` | Boolean | true | Whether to write header row with bold styling |
| `columns` | String/List | null | Column indices to include when writing |

*Either `file_path` or `output_stream` is required for write operations.

## Usage Examples

### Reading Excel Files

#### Basic Read

```java
ConnectorContext context = ConnectorContext.builder()
    .configuration(Map.of(
        "file_path", "data.xlsx"
    ))
    .build();

ExcelConnector connector = new ExcelConnector();
ConnectorResult result = connector.read(context);

List<Map<String, Object>> records = result.getRecords();
```

#### Read with Configuration

```java
ConnectorContext context = ConnectorContext.builder()
    .configuration(Map.of(
        "file_path", "data.xlsx",
        "sheet_name", "Sheet1",
        "start_row", 1,
        "limit", 100,
        "skip_empty_rows", true,
        "columns", "0,2-5,10"  // Include columns 0, 2, 3, 4, 5, and 10
    ))
    .build();

ExcelConnector connector = new ExcelConnector();
ConnectorResult result = connector.read(context);
```

#### Read from Classpath Resource

```java
ConnectorContext context = ConnectorContext.builder()
    .configuration(Map.of(
        "file_path", "classpath:data.xlsx"  // File in resources folder
    ))
    .build();

ExcelConnector connector = new ExcelConnector();
ConnectorResult result = connector.read(context);
```

#### Read from InputStream

```java
InputStream inputStream = new FileInputStream("data.xlsx");

ConnectorContext context = ConnectorContext.builder()
    .configuration(Map.of(
        "input_stream", inputStream,
        "sheet_index", 0
    ))
    .build();

ExcelConnector connector = new ExcelConnector();
ConnectorResult result = connector.read(context);
```

### Writing Excel Files

#### Basic Write

```java
List<Map<String, Object>> data = Arrays.asList(
    Map.of("name", "John", "age", 30, "city", "New York"),
    Map.of("name", "Jane", "age", 25, "city", "London")
);

ConnectorContext context = ConnectorContext.builder()
    .configuration(Map.of(
        "file_path", "output.xlsx",
        "sheet_name", "Users"
    ))
    .build();

ExcelConnector connector = new ExcelConnector();
ConnectorResult result = connector.write(context, data);
```

#### Write to OutputStream

```java
OutputStream outputStream = new FileOutputStream("output.xlsx");

ConnectorContext context = ConnectorContext.builder()
    .configuration(Map.of(
        "output_stream", outputStream,
        "sheet_name", "Data",
        "use_first_row_as_headers", true
    ))
    .build();

ExcelConnector connector = new ExcelConnector();
ConnectorResult result = connector.write(context, data);
```

### Streaming Operations

#### Stream Reading

```java
ConnectorContext context = ConnectorContext.builder()
    .configuration(Map.of(
        "file_path", "large-data.xlsx",
        "limit", 1000
    ))
    .build();

ExcelConnector connector = new ExcelConnector();

StreamObserver observer = new StreamObserver() {
    @Override
    public void onNext(Map<String, Object> record) {
        // Process each record
        System.out.println("Record: " + record);
    }

    @Override
    public void onComplete() {
        System.out.println("Stream completed");
    }

    @Override
    public void onError(Exception error) {
        System.err.println("Error: " + error.getMessage());
    }
};

StreamCancellable cancellable = connector.startStream(context, observer);

// To cancel the stream:
// cancellable.cancel();
```

#### Stream Writing

```java
ConnectorContext context = ConnectorContext.builder()
    .configuration(Map.of(
        "file_path", "output.xlsx"
    ))
    .build();

ExcelConnector connector = new ExcelConnector();

try (StreamWriter writer = connector.createWriter(context)) {
    // Write in batches
    writer.writeBatch(batch1);
    writer.writeBatch(batch2);
    writer.writeBatch(batch3);
}
```

## Column Selection Format

The `columns` configuration supports flexible column selection:

- **Single columns**: `"0,2,5"` - Includes columns 0, 2, and 5
- **Ranges**: `"0-5"` - Includes columns 0 through 5 (inclusive)
- **Mixed**: `"0,2-5,10"` - Includes columns 0, 2, 3, 4, 5, and 10
- **List format**: `[0, 2, 3, 4, 5, 10]` - Same as above using List

## Data Type Support

### Reading

The connector automatically handles the following Excel cell types:

- **String** - Text values
- **Numeric** - Numbers (automatically converted to Long for integers, Double for decimals)
- **Boolean** - Boolean values
- **Date** - Date values (automatically detected and converted to Date objects)
- **Formula** - Formula strings

### Writing

The connector supports writing the following Java types:

- **String** - Written as text
- **Number** (Integer, Long, Double, etc.) - Written as numeric values
- **Boolean** - Written as boolean values
- **Date** - Written as date values
- **null** - Written as blank cells

## API Methods

### DataSource Interface

- `read(ConnectorContext context)` - Read all data from Excel file
- `startStream(ConnectorContext context, StreamObserver observer)` - Stream data from Excel file

### DataSink Interface

- `write(ConnectorContext context, List<Map<String, Object>> data)` - Write data to Excel file

### DataStreamSink Interface

- `createWriter(ConnectorContext context)` - Create a StreamWriter for batch writing

## Error Handling

The connector provides detailed error messages:

- Missing configuration: "Missing source/destination: either 'file_path', 'input_stream' or 'output_stream' is required"
- File not found: "File not found: {file_path}"
- Sheet not found: "Sheet not found"
- Invalid column range: "Invalid column range: {range}"

## Performance Considerations

- **Streaming**: Use streaming operations for large files to avoid loading all data into memory
- **SXSSFWorkbook**: The writer uses Apache POI's SXSSFWorkbook with a row access window of 100 rows for memory efficiency
- **Column filtering**: Use column filtering to reduce memory usage when only specific columns are needed

## Dependencies

- Apache POI 5.5.0 (for Excel file handling)
- Universal Data Connectors SPI 0.0.2

## License

Apache License 2.0

## Author

Hai Pham Ngoc <ngochai285nd@gmail.com>

## Version

0.0.1
