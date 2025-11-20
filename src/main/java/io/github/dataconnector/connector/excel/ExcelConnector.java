package io.github.dataconnector.connector.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
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
    public StreamWriter createWriter(ConnectorContext arg0) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createWriter'");
    }

    @Override
    public ConnectorResult write(ConnectorContext context, List<Map<String, Object>> data) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'write'");
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

}
