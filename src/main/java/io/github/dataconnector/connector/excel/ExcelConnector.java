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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
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
import io.github.dataconnector.spi.model.ConnectorContext;
import io.github.dataconnector.spi.model.ConnectorMetadata;
import io.github.dataconnector.spi.model.ConnectorResult;
import io.github.dataconnector.spi.stream.StreamWriter;

public class ExcelConnector implements DataSource, DataSink, DataStreamSink {

    private static final Logger logger = LoggerFactory.getLogger(ExcelConnector.class);

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
            errors.add("Missing source: either 'file_path', 'input_stream' or 'output_stream' is required");
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

        try (InputStream inputStream = getInputStream(context)) {
            Workbook workbook = WorkbookFactory.create(inputStream);

            Sheet sheet = getSheet(context, workbook);
            if (sheet == null) {
                return ConnectorResult.builder()
                        .success(false)
                        .message("Sheet not found")
                        .build();
            }

            List<Map<String, Object>> records = parseSheet(sheet, context);

            return ConnectorResult.builder()
                    .success(true)
                    .message("Successfully read " + records.size() + " records from Excel file")
                    .recordsProcessed(records.size())
                    .records(records)
                    .executionTimeMillis(System.currentTimeMillis() - startTime)
                    .build();
        }
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

    private List<Map<String, Object>> parseSheet(Sheet sheet, ConnectorContext context) {
        List<Map<String, Object>> records = new ArrayList<>();
        Iterator<Row> rowIterator = sheet.rowIterator();

        int headerRowIndex = context.getConfiguration("header_row", Integer.class).orElse(0);
        for (int i = 0; i < headerRowIndex && rowIterator.hasNext(); i++) {
            rowIterator.next();
        }

        if (!rowIterator.hasNext()) {
            return records;
        }

        Row headerRow = rowIterator.next();
        List<String> headers = new ArrayList<>();
        for (Cell cell : headerRow) {
            headers.add(getCellValue(cell).toString());
        }

        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Map<String, Object> record = new LinkedHashMap<>();
            boolean hasData = false;

            for (int i = 0; i < headers.size(); i++) {
                Cell cell = row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                if (cell != null) {
                    record.put(headers.get(i), getCellValue(cell));
                    hasData = true;
                }
            }

            if (hasData) {
                records.add(record);
            }
        }

        return records;
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

            for (Map<String, Object> record : records) {
                Row row = sheet.createRow(currentRowIndex++);
                int cellIndex = 0;
                for (String header : headers) {
                    Cell cell = row.createCell(cellIndex++);
                    Object value = record.get(header);
                    setCellValue(cell, value);
                }
            }

        }

        private void initializeSheet(Set<String> keySet) {
            String sheetName = context.getConfiguration("sheet_name", String.class).orElse("Sheet1");
            this.sheet = workbook.createSheet(sheetName);
            this.headers = new ArrayList<>(keySet);

            Row headerRow = sheet.createRow(currentRowIndex++);
            CellStyle boldStyle = workbook.createCellStyle();
            Font font = workbook.createFont();
            font.setBold(true);
            boldStyle.setFont(font);

            int cellIndex = 0;
            for (String header : headers) {
                Cell cell = headerRow.createCell(cellIndex++);
                cell.setCellValue(header);
                cell.setCellStyle(boldStyle);
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
