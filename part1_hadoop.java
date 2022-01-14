/////////////////////
// PART 1 - HADOOP //
/////////////////////

// **************************************************************
// 1.  How many rides occurred per weekday?
// I changed the map function (replacing all):
String line = value.toString();
String[] split = line.split(",");
String day = split[1]; // split[1] is the column of days

if (!"tpep_pickup_dow".equals(day)) {
  context.write(new Text(day), one);
}
// **************************************************************



// **************************************************************
// 2.  Using a mapreduce operation, count the number of rows in the dataset
// I changed the map function (replacing all):
String line = value.toString();
StringTokenizer itr = new StringTokenizer(line);
while (itr.hasMoreTokens()) {
  itr.nextToken();
  context.write(word, one);
}

// And I change the Text word:
private Text word = new Text("rows");
// Result : rows 1001 (including header)
// **************************************************************



// **************************************************************
// 3.  What was the most common number of passengers?
// I changed the map function (replace all) to:
String line = value.toString();
String[] split = line.split(",");
if (!"passenger_count".equals(split[3])) {
  word.set(split[3].concat(" passenger/s"));
  context.write(word, one);
}
// Result:
// 0 passenger/s	5
// 1 passenger/s	608
// 2 passenger/s	235
// 3 passenger/s	62
// 4 passenger/s	45
// 5 passenger/s	35
// 6 passenger/s	10
// **************************************************************



// **************************************************************
// 4. How many rides (grouped per day) had a total amount greater than 20?
// all in map function:
String line = value.toString();
String[] split = line.split(",");
String totalAmount = split[split.length - 1];
if (!"total_amount".equals(totalAmount)) {
    float amount = Float.parseFloat(totalAmount);
    if (amount > 20) {   
      word.set(split[1]);
      context.write(word, one);
    }
}
// Result
// fri	22
// mon	16
// sat	19
// sun	59
// thu	24
// thur	27
// tue	19
// wed	28
// **************************************************************



// **************************************************************
// 5.  How many trips occurred on weekdays?
// In functtion map
String line = value.toString();
String[] split = line.split(",");
String day = split[1];
if (!"tpep_pickup_dow".equals(day)) {
    if (!"sat".equals(day) && !"sun".equals(day)) {      
      word.set(day);
      context.write(word, one);
    }
}  

// Result
// fri	99
// mon	94
// thu	101
// thur	118
// tue	105
// wed	126
// **************************************************************



// **************************************************************
// 6. How many trips occurred before 2pm?
// map function:
String line = value.toString();
String[] split = line.split(",");
String hr = split[2];
DateFormat dateFormat = new SimpleDateFormat("HH:mm");
if (!"tpep_journey_hr".equals(hr)) {
  try {   
    if (dateFormat.parse(hr).after(dateFormat.parse("14:00"))) {    
      context.write(word, one);
    }
  } catch (Exception e) {
  // error parsing
  }
}  

// also:
import java.text.DateFormat;
import java.text.SimpleDateFormat;

private Text word = new Text("trips");

// result:
// trips	371
// **************************************************************



// **************************************************************
// 7.  What is the average "fare amount" per day
// fare amount - index 10
// day - index 1

// I made the changes on the map function. See below:

// map function 1 - (corresponds to output 1)
String line = value.toString();
String[] split = line.split(",");
String day = split[1];
if (!"fare_amount".equals(split[10])) {  
  int amount = Integer.parseInt(split[10]);
  word.set(day);
  context.write(word, new IntWritable(amount));
}

// map function 2 - (corresponds to  output 2)
String line = value.toString();
String[] split = line.split(",");
String day = split[1];

if (!"tpep_pickup_dow".equals(day)) {
  context.write(new Text(day), one);
}

// **** BASH PART ******
// I needed to save the output 1 in output1.txt, and same for output 2
// for that I used hdfs dfs -copytoLocal, for example:
// hdfs dfs -copyToLocal /output1/part-r-00000 /output1.txt

// bash script 
// needed to run : apt install bc
// Find the bash script in part1_hadoop.sh


// Result:
// Day: fri - average fare amount: 4.53
// Day: mon - average fare amount: 4.29
// Day: sat - average fare amount: 4.46
// Day: sun - average fare amount: 4.73
// Day: thu - average fare amount: 4.51
// Day: thur - average fare amount: 4.82
// Day: tue - average fare amount: 4.36

// **************************************************************
