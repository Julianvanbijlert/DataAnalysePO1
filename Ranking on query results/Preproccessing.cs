using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection.Metadata;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Data.Sqlite;
using static System.Runtime.InteropServices.JavaScript.JSType;

//The first program will do some preprocessing on the data and/or the workload.
//During this phase, a meta database will be constructed and filled.
//This metadb will be used when answering the actual queries.

 namespace project;

/// <summary>
/// Data for the METADB. Has a PostingList (float) and IDF (int)
/// </summary>
public class MetaData
{
    public List<float> PostingList;
    /// <summary>
    /// number of documents in our collection that contain t
    /// </summary>
    public int DocumentFrequency;

    /// <summary>
    /// idf is a measure for the rareness of a term
    /// </summary>
    public double IDF;
    public MetaData()
    {
        PostingList = new List<float>();
    }

    public MetaData(int i)
    {
        PostingList = new List<float>();
        PostingList.Add(i);
        DocumentFrequency = 1;
    }
}
public class Preprocessing
{
    /*
    CREATE TABLE autompg (
    id integer NOT NULL,
    mpg real,
    cylinders integer,
    displacement real,
    horsepower real,
    weight real,
    acceleration real,
    model_year integer,
    origin integer,
    brand text, 
    model text,
    type text,
    PRIMARY KEY (id)
    );
     */

    //fill metadb met posting lists
    //fill metadb met idf's = log(N/dft) rareness of a term, log(lengte van posting list / lengte van de file)
    //attribute similarity = sim(T,Q) = sum to m: (S(t,q),
    //          waar S(ti,qj) = J(W(t),W(q) * QF(q))
    //                  waar J(W(t),W(q)) = W(t) and W(q) / |W(t)| or |W(q)| =  en QF(q) = 1 + log(Fq) waar Fq = aantal keer dat q voorkomt in Q


    public Dictionary<string, float> RQF = new Dictionary<string, float>();
    public Dictionary<(string, string), MetaData> Database = new Dictionary<(string, string), MetaData>();
    public Dictionary<string, List<string>> tablesforDictionary = new Dictionary<string, List<string>>();
    public string[] queries = new string[] {"id", "mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model_year", "origin", "brand", "model", "type" };


    public Dictionary<string, string[]> types = new Dictionary<string, string[]>();
    //"C:/Users/julia/OneDrive/Bureaublad/UniUtrecht/Leerjaar 2 2023-2024/P4 Data Analyse en Retrieval/Opdrachten/Ranking on query results/Dataset.db";
    public readonly string database = "Data Source=Dataset.db";
    public readonly string table = "autompg";
    public readonly string filepathDBfill = "../../../autompg.sql";
    public readonly string metadb = "Data Source=metadb.db";
    public readonly string filepathMetadb = "../../../metadb.db";
    public readonly string filepathQueries = "../../../queries.txt";
    public int N_totaldocuments;
    public int N_totalqueries;
    public int uniqueQueries;

    public void Processing()
    {
        
        ReFillDatabase(database);

        using (var conn = new SqliteConnection(database))
        {
            conn.Open();
            var command = conn.CreateCommand();




            command.CommandText = $"SELECT * FROM {table}";

            //nu processen we de data
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    /*
                    id integer NOT NULL,
                    mpg real,
                    cylinders integer,
                    displacement real,
                    horsepower real,
                    weight real,
                    acceleration real,
                    model_year integer,
                    origin integer,
                    brand text, 
                    model text,
                    type text,
                    */
                    
                    List<((string, string), int)> keyValues = Map(reader);
                    Reduce(keyValues);

                    
                }
            }

            CalculateIDF();
        }

        Console.WriteLine("Metadb Database will now be emptied");

        PrintDatabase(metadb);
        DropDatabase(metadb);


        //fill metadb
        Console.WriteLine("Filling metadb");
        DictionaryToDatabase(Database, metadb);
        PrintDatabase(metadb);

        Console.ReadLine();
    }

    public void ReFillDatabase(string databasePath)
    {
        PrintDatabase(database);
        Console.WriteLine("Dropping Database");
        DropDatabase(databasePath);


        Console.WriteLine("Filling Database");
        PrintDatabase(database);
        FillDatabase(databasePath);
    }

   

    public void FillDatabase(string databasePath)
    {
        using (var connection = new SqliteConnection(databasePath))
        {
            // Open connection
            connection.Open();

            // Get the queries and data
            //string[] queries = ProcesQueries(GetQueries());
            string[] data = GetDataset();
            string[] commands = data;//queries.Concat(data).ToArray();


            // Execute commands
            foreach (var commandText in commands)
            {
                if (!string.IsNullOrWhiteSpace(commandText))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = commandText;
                        command.ExecuteNonQuery();
                    }
                }
            }
            
        }

        Console.WriteLine($"Database '{database}' filled successfully.");

    }


    public string LookUpTable(string attribute)
    {
        throw (new NotImplementedException());
    }
    public string[] GetQueries()
    {
        string script = File.ReadAllText(filepathQueries);

        // Split script into individual commands
        return script.Split('\n');
    }

    public string[] ProcesQueries(string[] queries)
    {
        string[] result = new string[queries.Length - 2];


        N_totalqueries = int.Parse(queries[0].Split(" ")[0]);

        //optional denk ik
        uniqueQueries = int.Parse(queries[1].Split(" ")[0]);

        for (int i = 2; i < queries.Length; i++)
        {
            result[i] = ProcesQuery(queries[i]);
        }

        return result;
    }

    public string ProcesQuery(string query)
    {
        throw (new NotImplementedException());
    }

    public string[] GetDataset()
    {
        // Read SQL script file
        string script = File.ReadAllText(filepathDBfill);

        // Split script into individual commands
        return script.Split(';');
    }

    public void DropDatabase(string databasePath)
    {
        using (var connection = new SqliteConnection(databasePath))
        {
            connection.Open();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT name FROM sqlite_master WHERE type='table';";
                using (var reader = command.ExecuteReader())
                {
                    List<string> tableNames = new List<string>();

                    while (reader.Read())
                    {
                        string tableName = reader.GetString(0);
                        if (!string.IsNullOrWhiteSpace(tableName))
                            tableNames.Add(tableName);
                    }

                    foreach (var tableName in tableNames)
                    {
                        Console.WriteLine($"Dropping table '{tableName}'...");

                        using (var transaction = connection.BeginTransaction())
                        {
                            using (var dropCommand = connection.CreateCommand())
                            {
                                dropCommand.Transaction = transaction;
                                dropCommand.CommandText = $"DROP TABLE IF EXISTS {tableName};";
                                dropCommand.ExecuteNonQuery();
                            }

                            transaction.Commit();
                        }
                    }
                }
            }
        }

        Console.WriteLine($"All tables in database '{databasePath}' dropped successfully.");
    }


    static void PrintDatabase(string databasePath)
    {

        // Create a connection to the SQLite database
        using (var connection = new SqliteConnection(databasePath))
        {
            // Open the connection
            connection.Open();

            // Get a list of tables in the database
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT name FROM sqlite_master WHERE type='table';";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string tableName = reader.GetString(0);
                        PrintTable(connection, tableName);
                    }
                }
            }
        }
    }

    static void PrintTable(SqliteConnection connection, string tableName)
    {
        // Create a command to select all data from the table
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $"SELECT * FROM {tableName};";

            // Execute the command and print the table header
            using (var reader = command.ExecuteReader())
            {
                Console.WriteLine($"Table: {tableName}");

                // Print column names
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    Console.Write(reader.GetName(i) + "\t");
                }
                Console.WriteLine();

                // Print data
                while (reader.Read())
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        Console.Write(reader.GetValue(i) + "\t");
                    }
                    Console.WriteLine();
                }
                Console.WriteLine();
            }
        }
    }


    public void DictionaryToDatabase(Dictionary<(string, string), MetaData> chosenDictionary, string databasePath)
    {
       HashSet<string> tables = new HashSet<string>();
        using (var connection = new SqliteConnection(databasePath))
        {
            connection.Open();

            using (var command = connection.CreateCommand())
            {
                /*
                foreach (KeyValuePair<string, List<string>> entry in tableDictionary)
                {
                    string key = entry.Key;
                    List<string> value = entry.Value;

                    command.CommandText = $"CREATE TABLE {key} (id integer NOT NULL, {key} text, PRIMARY KEY (id));";
                    command.ExecuteNonQuery();
                }
                */

                foreach (KeyValuePair<(string, string), MetaData> entry in chosenDictionary)
                {
                    
                    string attribuut = entry.Key.Item1;
                    string invulling = entry.Key.Item2; //NoPoint(entry.Key.Item2);
                    string posting = ListToString(entry.Value.PostingList);
                    double IDF = entry.Value.IDF;
                    string typeInvulling = ReturnType(attribuut);

                    //create a table for each different type
                    if (!tables.Contains(attribuut))
                    {
                        command.CommandText = $"CREATE TABLE {attribuut} " +
                                              $"({attribuut} {typeInvulling}, " +
                                              $"Posting text, " +
                                              $"IDF real, " +
                                              $"PRIMARY KEY({attribuut})" + 
                                              $");";

                        command.ExecuteNonQuery();
                        tables.Add(attribuut);
                    }

                    command.CommandText = $"INSERT INTO {attribuut} ({attribuut}, Posting, IDF) VALUES ('{invulling}', '{posting}', '{IDF}')";
                    command.ExecuteNonQuery();
                }
            }
        }
    }


    public static string NoPoint(string attribuut)
    {
        string noPoint = attribuut.Replace(".", ",");
        return noPoint;
    }

    public static string ReturnType(string attribuut)
    {
        string typeInvulling;
        switch (attribuut)
        {
            //all integers
            case "id":
            case "cylinders":
            case "model_year":
            case "origin": typeInvulling = "integer"; break;

            //all reals
            case "displacement":
            case "horsepower":
            case "weight":
            case "acceleration":
            case "mpg": typeInvulling = "real"; break;

            default: typeInvulling = "text"; break;
        }

        return typeInvulling;
    }

    static object ToValue(string value, string type)
    {
        switch (type)
        {
            case "integer": return int.Parse(value);
            case "real": return float.Parse(value);
            default: return value;
        }
    }

    static string ListToString(List<float> list)
    {
        string result = "";
        foreach (float id in list)
        {
            result += id + " ";
        }

        return result;
    }


   

    public List<((string, string), int)> Map(SqliteDataReader reader)
    {
        N_totaldocuments++;
        List<((string, string), int)> keyValues = new List<((string, string), int)>();
        int id = reader.GetInt32(0);

        //de get strings van de reader zijn de invullingen van de attributen van een auto. bv type = cilinder, en de invulling = 4
        for (int i = 1; i < queries.Length; i++)
        {
            //queries zijn de types die we hebben (oftewel de attributen van een auto)
            keyValues.Add(((queries[i], reader.GetString(i)), id));
        }
        /*
           //niet boeinnd dit was gewoon ff om te kijken
           float mpg = reader.GetFloat(1);
           int cylinders = reader.GetInt32(2);
           float displacement = reader.GetFloat(3);
           float horsepower = reader.GetFloat(4);
           float weight = reader.GetFloat(5);
           float acceleration = reader.GetFloat(6);
           int model_year = reader.GetInt32(7);
           int origin = reader.GetInt32(8);
           string brand = reader.GetString(9);
           string model = reader.GetString(10);
           string type = reader.GetString(11);
           string text = $"{mpg} {cylinders} {displacement} {horsepower} {weight} {acceleration} {model_year} {origin} {brand} {model} {type}";
           */
       

        return keyValues;
    }


    public void Reduce(List<((string, string), int)> keyValues)
    {
        foreach (((string, string), int) keyValue in keyValues)
        {
            //add to postinglist
            if (Database.ContainsKey(keyValue.Item1))
            {
                Database[keyValue.Item1].PostingList.Add(keyValue.Item2);
                Database[keyValue.Item1].DocumentFrequency++;

            }
            else
            {
                Database[keyValue.Item1] = new MetaData(keyValue.Item2);
            }
        }
    }

    public void CalculateIDF()
    {
        foreach (KeyValuePair<(string,string), MetaData> data in Database)
        {
            data.Value.IDF = Math.Log(N_totaldocuments / data.Value.DocumentFrequency);
        }
    }


    
}
