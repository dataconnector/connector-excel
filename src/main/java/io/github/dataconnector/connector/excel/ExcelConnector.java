package io.github.dataconnector.connector.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.dataconnector.spi.DataSink;
import io.github.dataconnector.spi.DataSource;
import io.github.dataconnector.spi.DataStreamSink;
import io.github.dataconnector.spi.DataStreamSource;
import io.github.dataconnector.spi.model.ConnectorContext;
import io.github.dataconnector.spi.model.ConnectorMetadata;
import io.github.dataconnector.spi.model.ConnectorResult;
import io.github.dataconnector.spi.stream.StreamCancellable;
import io.github.dataconnector.spi.stream.StreamObserver;
import io.github.dataconnector.spi.stream.StreamWriter;

public class ExcelConnector implements DataSource, DataSink, DataStreamSource, DataStreamSink {

    private static final Logger logger = LoggerFactory.getLogger(ExcelConnector.class);

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Override
    public String getType() {
        return "excel";
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return ConnectorMetadata.builder()
                .name("Excel Connector")
                .description("Excel Connector is a connector that allows you to read and write Excel files")
                .version("0.0.1")
                .author("Hai Pham Ngoc <ngochai285nd@gmail.com>")
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> validateConfiguration(ConnectorContext context) {
        List<String> errors = new ArrayList<>();
        Map<String, Object> configuration = context.getConfiguration();
        if (configuration == null
                || (!configuration.containsKey("file_path") && !configuration.containsKey("input_stream")
                        && !configuration.containsKey("output_stream"))) {
            errors.add("Missing source/destination: either 'file_path', 'input_stream' or 'output_stream' is required");
        }
        return errors;
    }

    @Override
    public StreamWriter createWriter(ConnectorContext context) throws IOException {
        return new ExcelStreamWriter(context);
    }

    @Override
    public ConnectorResult write(ConnectorContext context, List<Map<String, Object>> data) throws Exception {
        long startTime = System.currentTimeMillis();

        if (data == null || data.isEmpty()) {
            return ConnectorResult.builder()
                    .success(true)
                    .message("No data to write")
                    .recordsProcessed(0)
                    .records(data)
                    .executionTimeMillis(System.currentTimeMillis() - startTime)
                    .build();
        }

        try (StreamWriter writer = createWriter(context)) {
            writer.writeBatch(data);
        }

        return ConnectorResult.builder()
                .success(true)
                .message("Successfully wrote " + data.size() + " records to Excel file")
                .recordsProcessed(data.size())
                .records(data)
                .executionTimeMillis(System.currentTimeMillis() - startTime)
                .build();
    }

    @Override
    public ConnectorResult read(ConnectorContext context) throws Exception {
        long startTime = System.currentTimeMillis();

        int startRow = context.getConfiguration("start_row", Integer.class).orElse(0);
        int limit = context.getConfiguration("limit", Integer.class).orElse(-1);
        Set<Integer> includedColumns = parseIncludedColumns(context);
        boolean skipEmptyRows = context.getConfiguration("skip_empty_rows", Boolean.class).orElse(true);

        try (InputStream inputStream = getInputStream(context)) {
            Workbook workbook = WorkbookFactory.create(inputStream);

            Sheet sheet = getSheet(context, workbook);
            if (sheet == null) {
                return ConnectorResult.builder()
                        .success(false)
                        .message("Sheet not found")
                        .build();
            }

            Iterator<Row> rowIterator = sheet.iterator();
            List<String> headers = extractHeaders(rowIterator, context);
            for (int i = 0; i < startRow && rowIterator.hasNext(); i++) {
                rowIterator.next();
            }

            List<Map<String, Object>> records = new ArrayList<>();
            while (rowIterator.hasNext()) {
                if (limit != -1 && records.size() >= limit) {
                    break;
                }

                Row row = rowIterator.next();
                if (skipEmptyRows && isRowEmpty(row)) {
                    continue;
                }

                Map<String, Object> record = parseRow(row, headers, includedColumns);
                records.add(record);
            }

            return ConnectorResult.builder()
                    .success(true)
                    .message("Successfully read " + records.size() + " records from Excel file")
                    .recordsProcessed(records.size())
                    .records(records)
                    .executionTimeMillis(System.currentTimeMillis() - startTime)
                    .build();
        }
    }

    @Override
    public StreamCancellable startStream(ConnectorContext context, StreamObserver observer) throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);

        Set<Integer> includedColumns = parseIncludedColumns(context);
        boolean skipEmptyRows = context.getConfiguration("skip_empty_rows", Boolean.class).orElse(true);
        int startRow = context.getConfiguration("start_row", Integer.class).orElse(0);

        executor.submit(() -> {
            try (InputStream inputStream = getInputStream(context)) {
                Workbook workbook = WorkbookFactory.create(inputStream);

                Sheet sheet = getSheet(context, workbook);
                if (sheet == null) {
                    throw new IllegalArgumentException("Sheet not found");
                }

                Iterator<Row> rowIterator = sheet.iterator();
                List<String> headers = extractHeaders(rowIterator, context);
                for (int i = 0; i < startRow && rowIterator.hasNext(); i++) {
                    rowIterator.next();
                }

                while (rowIterator.hasNext() && running.get()) {
                    Row row = rowIterator.next();
                    if (skipEmptyRows && isRowEmpty(row)) {
                        continue;
                    }

                    Map<String, Object> record = parseRow(row, headers, includedColumns);
                    observer.onNext(record);
                }
                logger.info("Excel stream reader completed");
                observer.onComplete();
            } catch (Exception e) {
                logger.error("Error in Excel stream reader", e);
                observer.onError(e);
            }
        });

        return () -> {
            logger.info("Requesting stream cancellation");
            running.set(false);
        };
    }

    private InputStream getInputStream(ConnectorContext context) throws Exception {
        String filePath = context.getConfiguration("file_path", String.class).orElse(null);
        Object inputStreamObject = context.getConfiguration().get("input_stream");

        if (inputStreamObject instanceof InputStream) {
            return (InputStream) inputStreamObject;
        } else if (filePath != null) {
            File file = new File(filePath);
            if (file.exists()) {
                return new FileInputStream(file);
            }

            URL url = getClass().getClassLoader().getResource(filePath);
            if (url != null) {
                logger.info("Reading Excel file from URL: {}", url.toString());
                return url.openStream();
            }
            throw new IllegalArgumentException("File not found: " + filePath);
        }
        throw new IllegalArgumentException("Missing input: either 'file_path' or 'input_stream' is required");
    }

    private Sheet getSheet(ConnectorContext context, Workbook workbook) {
        String sheetName = context.getConfiguration("sheet_name", String.class).orElse(null);
        Integer sheetIndex = context.getConfiguration("sheet_index", Integer.class).orElse(null);

        if (sheetName != null) {
            return workbook.getSheet(sheetName);
        } else if (sheetIndex != null) {
            return workbook.getSheetAt(sheetIndex);
        } else {
            return workbook.getSheetAt(0);
        }
    }

    private Object getCellValue(Cell cell) {
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case BOOLEAN:
                return cell.getBooleanCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue();
                } else {
                    double value = cell.getNumericCellValue();
                    if (value == (long) value) {
                        return (long) value;
                    }
                    return value;
                }
            case FORMULA:
                return cell.getCellFormula();
            default:
                return "";
        }
    }

    private Set<Integer> parseIncludedColumns(ConnectorContext context) {
        Object columnsConfig = context.getConfiguration().get("columns");
        if (columnsConfig == null) {
            return null;
        }

        Set<Integer> indices = new HashSet<>();
        try {
            if (columnsConfig instanceof String) {
                String[] parts = ((String) columnsConfig).split(",");
                for (String part : parts) {
                    part = part.trim();
                    if (part.contains("-")) {
                        String[] range = part.split("-");
                        if (range.length != 2) {
                            throw new IllegalArgumentException("Invalid column range: " + part);
                        }
                        int start = Integer.parseInt(range[0].trim());
                        int end = Integer.parseInt(range[1].trim());
                        for (int i = start; i <= end; i++) {
                            indices.add(i);
                        }
                    } else {
                        int index = Integer.parseInt(part);
                        indices.add(index);
                    }
                }
            } else if (columnsConfig instanceof List) {
                for (Object item : (List<?>) columnsConfig) {
                    indices.add(Integer.parseInt(item.toString()));
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to parse 'columns' config", e);
            return null;
        }
        return indices.isEmpty() ? null : indices;
    }

    private boolean isRowEmpty(Row row) {
        if (row == null || row.getLastCellNum() <= 0) {
            return true;
        }
        for (int c = row.getFirstCellNum(); c < row.getLastCellNum(); c++) {
            Cell cell = row.getCell(c);
            if (cell != null && cell.getCellType() != CellType.BLANK) {
                return false;
            }
        }
        return true;
    }

    private Map<String, Object> parseRow(Row row, List<String> headers, Set<Integer> includedColumns) {
        Map<String, Object> record = new LinkedHashMap<>();
        int maxCells = row.getLastCellNum();

        for (int i = 0; i < maxCells; i++) {
            if (includedColumns != null && !includedColumns.contains(i)) {
                continue;
            }

            String key = (i < headers.size()) ? headers.get(i) : "column_" + (i - headers.size() + 1);
            Cell cell = row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
            Object value = (cell == null) ? null : getCellValue(cell);
            record.put(key, value);
        }
        return record;
    }

    private List<String> extractHeaders(Iterator<Row> rowIterator, ConnectorContext context) {
        boolean useFirstRowAsHeaders = context.getConfiguration("use_first_row_as_headers", Boolean.class).orElse(true);
        List<String> headers = new ArrayList<>();

        if (useFirstRowAsHeaders && rowIterator.hasNext()) {
            Row headerRow = rowIterator.next();
            for (Cell cell : headerRow) {
                headers.add(getCellValue(cell).toString());
            }
        } else {
            //
        }
        return headers;
    }

    private class ExcelStreamWriter implements StreamWriter {

        private final ConnectorContext context;
        private SXSSFWorkbook workbook;
        private Sheet sheet;
        private OutputStream outputStream;
        private boolean isClosed = false;
        private int currentRowIndex = 0;
        private List<String> headers;

        public ExcelStreamWriter(ConnectorContext context) {
            this.context = context;
            this.workbook = new SXSSFWorkbook(100);
        }

        @Override
        public void close() throws IOException {
            if (isClosed) {
                return;
            }
            isClosed = true;

            try {
                String filePath = context.getConfiguration("file_path", String.class).orElse(null);
                Object outputStreamObject = context.getConfiguration().get("output_stream");

                if (outputStreamObject instanceof OutputStream) {
                    this.outputStream = (OutputStream) outputStreamObject;
                } else if (filePath != null) {
                    File file = new File(filePath);
                    if (file.getParentFile() != null && !file.getParentFile().exists()) {
                        if (!file.getParentFile().mkdirs()) {
                            throw new IOException("Failed to create parent directories for " + filePath);
                        }
                    }
                    this.outputStream = new FileOutputStream(file);
                } else {
                    throw new IllegalArgumentException(
                            "Missing output: either 'file_path' or 'output_stream' is required");
                }
                workbook.write(outputStream);
            } finally {
                workbook.close();

                if (outputStream != null && context.getConfiguration().containsKey("file_path")) {
                    outputStream.close();
                }
            }
        }

        @Override
        public void writeBatch(List<Map<String, Object>> records) throws IOException {
            if (isClosed) {
                throw new IOException("StreamWriter is already closed");
            }
            if (records == null || records.isEmpty()) {
                return;
            }

            if (sheet == null) {
                initializeSheet(records.get(0).keySet());
            }

            Set<Integer> includedColumns = parseIncludedColumns(context);

            for (Map<String, Object> record : records) {
                Row row = sheet.createRow(currentRowIndex++);
                int cellIndex = 0;

                for (int i = 0; i < headers.size(); i++) {
                    if (includedColumns != null && !includedColumns.contains(i)) {
                        continue;
                    }
                    String header = headers.get(i);
                    Cell cell = row.createCell(cellIndex++);
                    Object value = record.get(header);
                    setCellValue(cell, value);
                }
            }

        }

        private void initializeSheet(Set<String> keySet) {
            String sheetName = context.getConfiguration("sheet_name", String.class).orElse("Sheet1");
            boolean withHeader = context.getConfiguration("use_first_row_as_headers", Boolean.class).orElse(true);

            this.sheet = workbook.createSheet(sheetName);
            this.headers = new ArrayList<>(keySet);

            if (withHeader) {
                Row headerRow = sheet.createRow(currentRowIndex++);
                CellStyle boldStyle = workbook.createCellStyle();
                Font font = workbook.createFont();
                font.setBold(true);
                boldStyle.setFont(font);

                int cellIndex = 0;
                Set<Integer> includedColumns = parseIncludedColumns(context);
                for (int i = 0; i < headers.size(); i++) {
                    if (includedColumns != null && !includedColumns.contains(i)) {
                        continue;
                    }
                    Cell cell = headerRow.createCell(cellIndex++);
                    cell.setCellValue(headers.get(i));
                    cell.setCellStyle(boldStyle);
                }
            }

        }

        private void setCellValue(Cell cell, Object value) {
            if (value == null) {
                cell.setBlank();
            } else if (value instanceof Number) {
                cell.setCellValue(((Number) value).doubleValue());
            } else if (value instanceof Boolean) {
                cell.setCellValue((Boolean) value);
            } else if (value instanceof Date) {
                cell.setCellValue((Date) value);
            } else {
                cell.setCellValue(value.toString());
            }
        }
    }
}
