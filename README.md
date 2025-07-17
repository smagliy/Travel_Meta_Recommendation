# Travel Meta Recommendation

Motels.home is a European based, widely known travel company, selling online bookings for hotels, motels and other 
kinds of accommodation. A very important part of their business is dealing with meta partners.

In a nutshell, Motel.home wants to advertise it's hotels on several partners site. In order to do that Motels.home 
has to pay some money for a given motel to appear on the partner's site. This is called bidding.
To make this a bit easier, partners expose a file with the recommended bids for the motels, so Motels.home knows 
which is the recommended price to pay if they want their motel to be listed.

## 🧠 Available Tasks

Please find the prepared skeleton in the source directory. You only have to substitute the ???-s with proper logic. 
You can also find some example input/output files in the test resource directory, you can use those to validate your logic.

1. Erroneous records   
   Nothing is perfect neither is the bidding system. From time to time something goes wrong in the bidding file 
   generator in the partner's side, so it includes corrupted records in the stream.   
   A record is - corrupted if the value in the third column is not a number, but a text with this format: ERROR_(.*)   
   The task here is to filter out the corrupted records from the input and count how many occurred from the given type 
   in the given hour.   
   You may want to use BidError domain class. The output should be formatted as comma separated lines containing
   the date (with hour precision), the error message and the count of such messages in the given hour.
   See "expected\_core" under expected\_out for example.   

2. Exchange rates   
   As Motels.home is Europe based it is convenient to convert all types of currencies to EUR for the business analysts.
   In our example we have only USD so we will do only USD to EUR conversion. Here you have to read the currencies into 
   a map where the dates are the keys and the related conversion rates are the values. Use this data as mapping source 
   to be able to exchange the provided USD value to EUR on any given date/time.   

3. Dealing with bids   
   Now we can focus on the most important parts, the bids. In Task 1 you have read the original bids data. 
   The first part is to get rid of the erroneous records and keep only the conforming ones which are not prefixed 
   with ERROR_ strings.    
   In this campaign Motel.home is focusing only on three countries: US,CA,MX so you'll have to only work with those three 
   and also the records have to be transposed so one record will only contain price for one Losa.   
   .    
   Example:   
  - original record:   
   ["MotelID", "BidDate", "HU", "UK", "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE"]  
   [0000002,11-05-08-2016,0.92,1.68,0.81,0.68,1.59,,1.63,1.77,2.06,0.66,1.53,,0.32,0.88,0.83,1.01]   
  - keep only the three important ones   
  0000002,11-05-08-2016,1.59,,1.77
  - transpose the record and include the related Losa in a separate column   
  0000002,11-05-08-2016,US,1.59   
  0000002,11-05-08-2016,MX,   
  0000002,11-05-08-2016,CA,1.77   
  (This is closely related to SQL explode functionality)
   .   
   You may want to use BidItem domain class to represent the records.   
   .   
   Somewhere in this task you have to do the following conversions/filters:
  - Convert USD to EUR. The result should be rounded to 3 decimal precision.
  - Convert dates to proper format - use formats in Constants util class
  - Get rid of records where there is no price for a Losa or the price is not a proper decimal number

4. Load motels   
   Load motels data and prepare it for joining with bids.   
   Hint: we want to enrich the bids data with motel names, so you'll probably need the motel id and motel name as well.

5. Finally enrich the data and find the maximum   
   Motels.home wants to identify rich markets so it is interested where the advertisement is the most expensive 
   so we are looking for maximum values.   
   Join the bids with motel names. You can use EnrichedItem domain class to represent the join record.    
   As a final output we want to find and only keep the records which have the maximum prices for a given motelId/bidDate.
   When determining the maximum if the same price appears twice then keep the first object you found with the given price.
   .   
   Example:   
  - from records   
  0000001,Fantastic Hostel,2016-06-02 11:00,MX,1.50   
  0000001,Fantastic Hostel,2016-06-02 11:00,US,1.40   
  0000001,Fantastic Hostel,2016-06-02 11:00,CA,1.15   
  0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,MX,1.10   
  0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,US,1.20   
  0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,CA,1.30   
  - you will have to keep   
  0000001,Fantastic Hostel,2016-06-02 11:00,MX,1.50   
  0000005,Majestic Ibiza Por Hostel,2016-06-02 12:00,CA,1.30

## 📁 Project Structure

<pre> 
├── src/
│ ├── config/settings.py
│ ├── services/
│ │ ├── log/logger.py
│ │ ├── read/reader.py
│ │ └── write/writer.py
│ ├── tasks/
│ │ ├── biddata_processor.py
│ │ ├── currency_conversion.py
│ │ ├── load_motels.py
│ │ ├── transform_bids.py
│ │ └── max_motels_processor.py
│ └── utils/file_helper.py
└── main.py
</pre> 
---

## ✅ Requirements

- Python 3.8+
- Recommended to use a virtual environment

Install dependencies:

```bash
pip install -r requirements.txt
```

## 📄 Required Input Files
Place these files inside the data/ folder:

- bids.txt

- exchange_rates.txt

- motels.txt

The column headers for each file are defined in src/config/settings.py.

## 🚀 How to Run
Run the main entry script:

```bash
python main.py
```
Then enter a number from 1 to 5 based on the task you'd like to run.

## 📦 Output
Each task will output a .parquet file in the output/ directory (created automatically if missing):

<pre> 
output/
└── task_<number>.parquet
</pre> 

## 🛠️ Logging
Logs are printed to the console and can be configured in LoggerConfig inside src/services/log/logger.py.

## 📝 Example
```bash
$ python main.py
Enter the relative path from the root to the folder with your files: data
Enter task number (1-5): 3
INFO: Running Task 3: Full Currency Conversion
INFO: Task 3 completed successfully. Output saved to: /your/path/output/task_3.parquet
```
## 📬 Notes
Make sure your input text files are correctly delimited and match the expected headers.

Parquet output overwrites existing files with the same name.

