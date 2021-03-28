package com.jai.stock;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Stream;

import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellUtil;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.io.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RobinhoodHistory {

	private static Logger log = LoggerFactory.getLogger(RobinhoodHistory.class);

	public static final String DELIMITER = "\t";

	public static void main(String[] args) {

		String inputFile = ".\\src\\test\\resources\\sample_robinhood_history_template1.txt";
		Path inp = Paths.get(inputFile);
		Path out =   Paths.get(inp.getParent() + "/" + inp.getFileName() + "_" + System.currentTimeMillis() + ".xlsx" );
		RobinhoodHistory r = new RobinhoodHistory();
		r.extractToXlsx(inp, out);

	}

	private void extractToXlsx(Path inp, Path out) {

		try {
			String content = readLineByLineJava8(inp.toString());
			String content1 = content.replaceAll("\r\n\r\n", "||");
			String content2 = content1.replaceAll("\r\n", DELIMITER);
			String content3 = content2.replaceAll("\\|\\|", "\r\n");

			String content4 = content3.replaceAll("Market Buy", DELIMITER + "Market" + DELIMITER + "Buy");
			String content5 = content4.replaceAll("Market Sell", DELIMITER + "Market" + DELIMITER + "Sell");
			String content6 = content5.replaceAll("Limit Buy", DELIMITER + "Limit" + DELIMITER + "Buy");
			String content7 = content6.replaceAll("Limit Sell", DELIMITER + "Limit" + DELIMITER + "Sell");
			String content8 = content7.replaceAll("Market Buy", DELIMITER + "Market" + DELIMITER + "Buy");
			String content9 = content8.replaceAll("Trailing Stop Buy",
					DELIMITER + "Trailing" + DELIMITER + "Stop" + DELIMITER + "Buy");
			String content10 = content9.replaceAll("Trailing Stop Sell",
					DELIMITER + "Trailing" + DELIMITER + "Stop" + DELIMITER + "Sell");

			String content11 = content10.replaceAll("Deposit from ", "Deposit" + DELIMITER + "Deposit"+ DELIMITER);
			String content12 = content11.replaceAll("^\tRecent$", "Recent");

			Workbook wb = new XSSFWorkbook();

			CreationHelper helper = wb.getCreationHelper();
			Sheet sheet = wb.createSheet("sheet1");

			List<String> lines = IOUtils.readLines(new StringReader(content12));

			for (int i = 0; i < lines.size(); i++) {
				String str[] = lines.get(i).split(DELIMITER);
				Row row = sheet.createRow((short) i);
				for (int j = 0; j < str.length; j++) {
					String text = str[j];
					row.createCell(j).setCellValue(helper.createRichTextString(text));

				}
			}
			
			for (int rowIndex = 0; rowIndex<sheet.getPhysicalNumberOfRows(); rowIndex++){
			    Row row = CellUtil.getRow(rowIndex, sheet);
			    Cell c = CellUtil.getCell(row, 5);
			    String v = c.getStringCellValue();
			    
			    String[] split =    ( v!=null ) ?  	v.split("at"):null;
			    
			    
			    log.info("{}",c.getAddress());
			}
			
			
			

			try (FileOutputStream fileOut = new FileOutputStream(out.toString())) {
				log.info("Writing output : {}", out.toFile().getAbsolutePath());
				wb.write(fileOut);
				fileOut.close();
			}

			/*
			 * String content13 = content12.replaceAll("||", "\r\n"); String content11 =
			 * content10.replaceAll("||", "\r\n"); String content12 =
			 * content11.replaceAll("\r\n", DELIMITER); String content13 =
			 * content12.replaceAll("||", "\r\n");
			 * 
			 * String content11 = content10.replaceAll("||", "\r\n"); String content12 =
			 * content11.replaceAll("\r\n", DELIMITER); String content13 =
			 * content12.replaceAll("||", "\r\n");
			 * 
			 * String content11 = content10.replaceAll("||", "\r\n"); String content12 =
			 * content11.replaceAll("\r\n", DELIMITER); String content13 =
			 * content12.replaceAll("||", "\r\n");
			 */

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	 private static String readLineByLineJava8(String filePath) 
	    {
	        StringBuilder contentBuilder = new StringBuilder();
	 
	        try (Stream<String> stream = Files.lines( Paths.get(filePath), StandardCharsets.UTF_8)) 
	        {
	            stream.forEach(s -> contentBuilder.append(s).append("\n"));
	        }
	        catch (IOException e) 
	        {
	            e.printStackTrace();
	        }
	 
	        return contentBuilder.toString();
	    }
	}
