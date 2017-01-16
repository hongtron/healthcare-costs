package edu.uchicago.mpcs53013.alih.HdfsIngestProcedure;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import edu.uchicago.mpcs53013.alih.HdfsIngestProcedure.ProcedureSummary;


public abstract class ProcedureSummaryProcessor {
	static class MissingDataException extends Exception {

	    public MissingDataException(String message) {
	        super(message);
	    }

	    public MissingDataException(String message, Throwable throwable) {
	        super(message, throwable);
	    }

	}
	
	static double tryToReadMeasurement(String name, String s, String missing) throws MissingDataException {
		if(s.equals(missing))
			throw new MissingDataException(name + ": " + s);
		return Double.parseDouble(s.trim());
	}

	void processRecord(CSVRecord record) throws IOException {
		try {
			processProcedureSummary(procedureFromRecord(record));
		} catch(MissingDataException e) {
			// Just ignore lines with missing data
		}
	}

	abstract void processProcedureSummary(ProcedureSummary summary) throws IOException;

	Iterable<CSVRecord> getCsvIterator(File file) throws FileNotFoundException, IOException {
		Reader in = new FileReader(file);
		Iterable<CSVRecord> records = CSVFormat.TDF.withFirstRecordAsHeader().parse(in);
		return records;
	}
	
	void processFile(File file) throws IOException {		
		Iterable<CSVRecord> records = getCsvIterator(file);

		for (CSVRecord record : records) {
		    processRecord(record);
		}
	}

	void processDirectory(String directoryName) throws IOException {
		File directory = new File(directoryName);
		File[] directoryListing = directory.listFiles();
		for(File file : directoryListing)
			processFile(file);
	}
	
	ProcedureSummary procedureFromRecord(CSVRecord record) throws NumberFormatException, MissingDataException {
		ProcedureSummary summary 
			= new ProcedureSummary(record.get("DRG Definition").substring(0, 3),
								   record.get("DRG Definition").substring(6).replace(",", " "),
				                   record.get("Provider Name"),
				                   record.get("Provider Zip Code"),
				                   record.get("Provider State"),
				                   Double.parseDouble(record.get("Average Covered Charges").substring(1)),
				                   Double.parseDouble(record.get("Average Total Payments").substring(1)),
				                   Double.parseDouble(record.get("Average Medicare Payments").substring(1)),
				                   Integer.parseInt(record.get("Total Discharges"))
				                   );
		return summary;
	}

}
