using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nuve.Tokenizers;
using System.Collections;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Easy.Common.Extensions;
using MPI;
namespace ConsoleApp1
{
    class Source
    {
        static void Main(string[] args)
        {
            MPI.Environment.Run(ref args, communicator =>
            {
                if (communicator.Rank == 0) //Master process
                {
                    Console.WriteLine("Process with rank " + communicator.Rank + " successfully running. Total rank " + communicator.Size);
                    
                    //Timer
                    System.Diagnostics.Stopwatch watch = System.Diagnostics.Stopwatch.StartNew();
                    
                    long totalLineNumber = 0;
                    long intervalForEachProcess = 0;
                    
                    FileStream readInput = File.OpenRead("news.txt");
                    totalLineNumber = StreamExtensions.CountLines(readInput, default);
                    Console.WriteLine("Total line number in the input text: " + totalLineNumber);

                    intervalForEachProcess = totalLineNumber / (communicator.Size - 1);
                    long startIndexOfEachProcess = 0;
                    //Reading number of lines in the input, and distributing them equally to all machines in the cluster
                    for (int rank = 1; rank < communicator.Size; rank++)
                    {    
                        communicator.Send(startIndexOfEachProcess, rank, 0);
                        if(rank != communicator.Size - 1)
                        {
                            communicator.Send(startIndexOfEachProcess + intervalForEachProcess, rank, 0);
                        }
                        else
                        {
                            //Simply add the remaining part to last process
                            communicator.Send(totalLineNumber, rank, 0); 
                        }
                        startIndexOfEachProcess += intervalForEachProcess;
                    }
                    
                    //BiGram - count
                    Dictionary<String, int> hashMap = new Dictionary<String, int>();

                    int terminator = 0;
                    while (terminator != communicator.Size - 1)
                    {                     
                        String recievedBiGramString = communicator.Receive<String>(MPI.Communicator.anySource, 0);
                        //Console.WriteLine(recievedBiGramString);
                        if (recievedBiGramString == "###-TERMINATION-###")
                        {
                            terminator++;
                        }
                        else if(!hashMap.ContainsKey(recievedBiGramString))
                        {
                            hashMap.Add(recievedBiGramString,1);
                        }
                        else
                        {
                            hashMap[recievedBiGramString]++;
                        }
                    }
                    Console.WriteLine("Loop terminated.");
                    
                    //Handles with biGrams which are obtained from last word of an interval and first word of next interval
                    for (int i = 1; i <communicator.Size - 1; i++)
                    {
                        String firstWordOfInterval = communicator.Receive<String>(i, i);                       
                        String lastWordOfLine = communicator.Receive<String>(i + 1, i+1);                     
                        String recievedBiGramString = lastWordOfLine + " " + firstWordOfInterval;

                        Console.WriteLine(recievedBiGramString + " " + i + " " + (i + 1));

                        if (!hashMap.ContainsKey(recievedBiGramString))
                        {
                            hashMap.Add(recievedBiGramString, 1);
                        }
                        else
                        {
                            hashMap[recievedBiGramString]++;
                        }
                    }
                    Console.WriteLine("Separation points done.");

                    watch.Stop();
                    long elapsedMs = watch.ElapsedMilliseconds;
                    long elapsedSecond = elapsedMs / 1000;
                    Console.WriteLine("biGram analysis time: " + elapsedSecond / 60 + "  min" + elapsedSecond % 60 + " sec");

                    watch.Start();
                    KeyValuePair<String, int>[] output = countSort(hashMap);
                    watch.Stop();

                    long overall = watch.ElapsedMilliseconds;
                    long overallSec = overall / 1000;
                    Console.WriteLine("Sort time: " + (overallSec - elapsedSecond) / 60 + "  min" + (overallSec - elapsedSecond) % 60 + " sec");
                    Console.WriteLine("Overall time: " + overallSec / 60 + "  min" + overallSec % 60 + " sec");

                    StreamWriter writer = File.CreateText("out.txt");
                    for(int i = 0; i < output.Length; i++)
                    {
                        writer.WriteLine(output[i]);
                    }            
                    writer.Close();

                    Console.WriteLine("Output.txt has been written successfully.");

                }
                else // Processes that makes bigram analysis
                {
                    Console.WriteLine("Process with rank " + communicator.Rank + " successfully running.");
                    long intervalStart = communicator.Receive<long>(0, 0);
                    long intervalEnd = communicator.Receive<long>(0, 0);
                    Console.WriteLine(intervalStart + " " + intervalEnd);               
                    
                    StreamReader readInput = new StreamReader("news.txt");

                    //Carries readStream's current index to the given line number by master process.
                    for (long i = 0; i < intervalStart; i++)
                    {
                        readInput.ReadLine();
                    }

                    NuveTokenizer tokenize = new NuveTokenizer(false);
                    IList<String> tokenizedInput;

                    String firstWordOfInterval = " ";
                    bool isFirstWord = true;

                    // Last word of each line is temporarily held in this variable to be matched with first word of next line
                    // Otherwise, 1 biGram will be lost by every line
                    // Also used for holding last word of each interval too
                    String lastWordOfLine = " ";

                    //The main loop which makes bigram analysis
                    for (long i = intervalStart; i < intervalEnd; i++)
                    {
                        tokenizedInput = tokenize.Tokenize(readInput.ReadLine().ToString());
                        //Instead of natural language processing library Nuve, simply following line could have been used.
                        //tokenizedInput = readInput.ReadLine().ToString().Split(); 
                        
                        if (isFirstWord && tokenizedInput.IsNotNullOrEmpty())
                        {
                            firstWordOfInterval = tokenizedInput[0];
                            isFirstWord = false;
                        }

                        if (tokenizedInput.IsNotNullOrEmpty() && lastWordOfLine != " ")
                        {
                            String endLineStartingLineMergedBiGram = tokenizedInput[0] + " " + lastWordOfLine;
                            communicator.Send(endLineStartingLineMergedBiGram, 0, 0);
                        }

                        for (int k = 0; k < tokenizedInput.Count - 1; k++)
                        {
                            String biGram = tokenizedInput[k] + " " + tokenizedInput[k +1];
                            communicator.Send(biGram, 0, 0);
                            lastWordOfLine = tokenizedInput[k + 1];
                        }
                    }                 
                    communicator.Send("###-TERMINATION-###", 0, 0);
                    //Console.WriteLine("###-TERMINATION-### sent from: " + communicator.Rank);
                    
                    //Handles with biGrams which are obtained from last word of an interval and first word of next interval
                    if (communicator.Rank == 1)
                    {
                        Console.WriteLine("Trying to send from " + communicator.Rank);
                        communicator.Send(lastWordOfLine, 0, communicator.Rank); 
                    }
                    else if (communicator.Rank == communicator.Size - 1)
                    {
                        Console.WriteLine("Trying to send from " + communicator.Rank);
                        communicator.Send(firstWordOfInterval, 0, communicator.Rank);
                    }
                    else
                    {
                        Console.WriteLine("Trying to send from " + communicator.Rank);
                        communicator.Send(firstWordOfInterval, 0, communicator.Rank);
                        communicator.Send(lastWordOfLine, 0, communicator.Rank);
                    }
                }     
            });         
        }

        public static KeyValuePair<String, int>[] countSort(Dictionary<String, int> hashMap)
        {
            //Range of inputs
            int max = hashMap.ElementAt(0).Value;
            for (int i = 1; i < hashMap.Count; i++)
            {
                if (hashMap.ElementAt(i).Value > max)
                {
                    max = hashMap.ElementAt(i).Value;
                }
            }
            int rangeOfInputElements = max + 1;

            // create an integer array of size range
            int[] freq = new int[rangeOfInputElements];

            // using value of integer in the input array as index,
            // store count of each integer in freq[] array
            for (int i = 0; i < hashMap.Count; i++)
            {
                freq[hashMap.ElementAt(i).Value]++;
            }

            // calculate the starting index for each integer
            int total = 0;
            for (int i = 0; i < rangeOfInputElements; i++)
            {
                int oldCount = freq[i];
                freq[i] = total;
                total += oldCount;
            }

            // create an integer array of size n to store sorted array
            KeyValuePair<String,int>[] output = new KeyValuePair<String, int>[hashMap.Count + 1];
            for (int i = 0; i < hashMap.Count; i++)
            {
                output[freq[hashMap.ElementAt(i).Value]] = hashMap.ElementAt(i);
                freq[hashMap.ElementAt(i).Value]++;
            }
            return output;
        }

        //I wrote rest of code just comparing performaces of C# HashMap and my binary insertion list.
        //C#'s HashMap won by very big difference. It was around 30 times faster for 20k lines string input.
        /*
        public static biGramObject[] countSort(biGramAlphabeticallySortedList biGramAlphabeticallySortedList)
        {
            //Range of inputs
            int max = biGramAlphabeticallySortedList.biGrams.ElementAt(0).count;
            for (int i = 1; i < biGramAlphabeticallySortedList.biGrams.Count; i++)
            {
                if (biGramAlphabeticallySortedList.biGrams.ElementAt(i).count > max)
                {
                    max = biGramAlphabeticallySortedList.biGrams.ElementAt(i).count;
                }
            }
            int rangeOfInputElements = max + 1;

            // create an integer array of size range
            int[] freq = new int[rangeOfInputElements];

            // using value of integer in the input array as index,
            // store count of each integer in freq[] array
            for (int i = 0; i < biGramAlphabeticallySortedList.biGrams.Count; i++)
            {
                freq[biGramAlphabeticallySortedList.biGrams.ElementAt(i).count]++;
            }

            // calculate the starting index for each integer
            int total = 0;
            for (int i = 0; i < rangeOfInputElements; i++)
            {
                int oldCount = freq[i];
                freq[i] = total;
                total += oldCount;
            }

            // create an integer array of size n to store sorted array
            biGramObject[] output = new biGramObject[biGramAlphabeticallySortedList.biGrams.Count + 1];
            for (int i = 0; i < biGramAlphabeticallySortedList.biGrams.Count; i++)
            {
                output[freq[biGramAlphabeticallySortedList.biGrams.ElementAt(i).count]] = biGramAlphabeticallySortedList.biGrams.ElementAt(i);
                freq[biGramAlphabeticallySortedList.biGrams.ElementAt(i).count]++;
            }
            return output;
        }

        public class biGramObject
        {
            public String[] biGram;
            public int count;

            public biGramObject(String[] biGram)
            {
                this.biGram = biGram;
                count = 1;
            }
        }

        public class biGramAlphabeticallySortedList
        {
            //Stores all biGrams in alphabetical order. 
            //I thought, in terms of complexity, millions times logn must be smaller than n^n.
            public List<biGramObject> biGrams = new List<biGramObject>();

            public void binaryInsert(String[] biGram)     // Inserts given biGram to correct(binary search list) index 
            {
                int index = findInsertIndex(biGram);

                if (index != -1)
                {
                    biGrams.Insert(index, new biGramObject(biGram));
                }
            }

            private int findInsertIndex(String[] biGram)     // Finds correct place for the given string that will be insterted to sorted list
            {
                int lowerBound = 0;
                int upperBound = biGrams.Count - 1;

                while (true)
                {
                    int currentInsert = (upperBound + lowerBound) / 2;
                    if (biGrams.Count == 0)
                    {
                        return currentInsert = 0;
                    }
                    if (lowerBound == currentInsert)
                    {
                        if (String.Compare(string.Join("", biGrams[currentInsert].biGram), string.Join("", biGram), true) > 0)
                        {
                            return currentInsert;
                        }
                    }
                    if (String.Compare(string.Join("", biGrams[currentInsert].biGram), string.Join("", biGram), true) < 0)
                    {
                        lowerBound = currentInsert + 1;          // its in the upper 
                        if (lowerBound > upperBound)
                        {
                            return currentInsert += 1;
                        }
                    }
                    else if (lowerBound > upperBound)
                    {
                        return currentInsert;
                    }
                    else
                    {
                        upperBound = currentInsert - 1;          // its in the lower   
                    }
                    if (String.Compare(string.Join("", biGrams[currentInsert].biGram), string.Join("", biGram), true) == 0)
                    {
                        //If biGram already exist in the list, then increase its count.
                        biGrams[currentInsert].count++;
                        return -1;
                    }
                }
            }
        }*/
    }
}
