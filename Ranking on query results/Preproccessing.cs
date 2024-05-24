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
using Boolean = System.Boolean;

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



    public float QF;

    /// <summary>
    /// idf is a measure for the rareness of a term
    /// </summary>
    public double IDF;


    //Emma weet dit
    public double h;

    public int RQF;
    public MetaData()
    {
        PostingList = new List<float>();
        DocumentFrequency = 0;
        RQF = 0;
    }

    public MetaData(int i)
    {
        PostingList = new List<float>(){i};
        DocumentFrequency = 1;
        RQF = 0;
    }

    public MetaData(List<float> postingList, int documentFrequency, double idf, int rqf)
    {
        PostingList = postingList;
        DocumentFrequency = documentFrequency;
        IDF = idf;
        RQF = rqf;
        
    }

    public MetaData(string pl, string idf, string rqf, string qf)
    {
        PostingList = QueryProccessing.StringToList(pl);
        

        IDF = double.Parse(idf);
        RQF = int.Parse(rqf);
        QF = float.Parse(qf);
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

    


    public static string[] queries = new string[] {"id", "mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model_year", "origin", "brand", "model", "type" };


    //"C:/Users/julia/OneDrive/Bureaublad/UniUtrecht/Leerjaar 2 2023-2024/P4 Data Analyse en Retrieval/Opdrachten/Ranking on query results/Dataset.db";
    public readonly string database = "Data Source=Dataset.db";
    public readonly string table = "autompg";
    public readonly string filepathDBfill = "../../../autompg.sql";
    public readonly string metadb = "Data Source=metadb.db";
    public readonly string filepathMetadb = "../../../metadb.db";
    public readonly string filepathQueries = "../../../queries.txt";

    private readonly bool RedoDB = true;
    private readonly bool RedoMDB = true;
    public int N_totaldocuments = 0;
    public int N_totalqueries = 0;
    public int uniqueQueries = 0;

    public int[] RQFMax = new int[queries.Length - 1];


    public void Processing()
    {
        if (RedoDB)
        {
            ReFillDatabase(database);

        }

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



            ProcesQueries(GetQueries());
            CalculateIDF();
            CalculateQF();
        }

        if (RedoMDB)
        {

            Console.WriteLine("Metadb Database will now be emptied");

             PrintDatabase(metadb);
             DropDatabase(metadb);


            //fill metadb
            Console.WriteLine("Filling metadb");
            DictionaryToDatabase(Database, metadb);
        }

        PrintDatabase(metadb);
        //Console.ReadLine();
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
            string[] commands = GetDataset();


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

    public void ProcesQueries(string[] queries)
    {
        string[] result = new string[queries.Length];


        N_totalqueries = int.Parse(queries[0].Split(" ")[0]);

        //optional denk ik
        uniqueQueries = int.Parse(queries[1].Split(" ")[0]);


        List<List<((string, string), int)>> mapped = new List<List<((string, string), int)>>();
        for (int i = 2; i < queries.Length - 1; i++)
        {
            mapped.Add(MapQuery(queries[i]));
        }

        foreach (List<((string, string), int)> map in mapped)
        {
            foreach (((string, string), int) keyValue in map)
            {
                if (Database.ContainsKey(keyValue.Item1))
                {
                    Database[keyValue.Item1].RQF += keyValue.Item2;
                }
                else
                {
                    //throw(new NotImplementedException());
                    Database[keyValue.Item1] = new MetaData();
                    Database[keyValue.Item1].RQF += keyValue.Item2;
                }

                if (RQFMax[AttributeToIndex(keyValue.Item1.Item1)] < keyValue.Item2)
                {
                    RQFMax[AttributeToIndex(keyValue.Item1.Item1)] = keyValue.Item2;
                }
            }
        }

    }

    

    public static List<((string, string), int)> MapQuery(string query)
    {
        //124 times: SELECT * FROM autompg WHERE model_year = '82' AND type = 'sedan'
        string[] parts = query.Split(" ");
        int aantal = 1;
        if (parts[0] != "SELECT")
            aantal = int.Parse(parts[0]);

        int i = 3;

        // Process the select part
        /*
        while (parts[i] != "FROM" && i < parts.Length)
        {
            attributen[i-3] = NoPoint(parts[i]);
            i++;
        }

        if (attributen[0] == "*")
        {
            attributen = queries;
        }
        */

        while (parts[i] != "WHERE" && i < parts.Length - 1) i++;

        i++;
        
        List<((string, string), int)> map = new List<((string, string), int)>();
        

        // Process the where part
        while (i < parts.Length - 1)
        {
            //attribute
            string attribute = NoPoint(parts[i]).Trim();
            

            //IN or =
            if (parts[i+1] == "IN")
            {
                //set catagorial values
                string[] values = parts[i + 2].Split(",");

                foreach (string valuez in values)
                {
                   string value = NoPoint(valuez.Replace("\r", "").Replace("\n", "").Replace("'", "").Trim());
                    map.Add(((attribute, value), aantal));
                    i++;
                }
                
                i++;
            }
            else if (parts[i + 1] == "=")
            {
                //catagorial value
                string value = NoPoint(parts[i + 2].Replace("\r", "").Replace("\n", "").Replace("'", "").Trim());
                map.Add(((attribute, value), aantal));
                i++;
            }
            else throw(new NotImplementedException());

            //set catagorial values or catagorial value

            i += 3;
        }



        return map;

        //throw (new NotImplementedException());
    }

    public string[] GetPosting(List<((string, string), int)> query)
    {
        string[] result = new string[query.Count];
        for (int i = 0; i < query.Count; i++)
        {
            result[i] = GetPostingDatabase(query[i].Item1.Item1);
            
        }

        return result;
    }

    public string GetPostingDatabase(string attribute)
    {
        
        using (var conn = new SqliteConnection(database))
        {
            conn.Open();
            var command = conn.CreateCommand();




            command.CommandText = $"SELECT {attribute}, Posting FROM {attribute}";

           

            //nu processen we de data
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {

                }
            }
        }

        return "";
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
                foreach (KeyValuePair<(string, string), MetaData> entry in chosenDictionary)
                {
                    string attribuut = entry.Key.Item1;
                    string typeInvulling = ReturnType(attribuut);
                    string invulling = entry.Key.Item2;
                    string posting = ListToString(entry.Value.PostingList);
                    double IDF = entry.Value.IDF;
                    int RQF = entry.Value.RQF;
                    float QF = entry.Value.QF;

                    // Create a table for each different type if it doesn't exist
                    if (!tables.Contains(attribuut))
                    {
                        command.CommandText = $"CREATE TABLE IF NOT EXISTS {attribuut} (" +
                                              $"{attribuut} {typeInvulling}," +
                                              $"Posting TEXT," +
                                              $"IDF REAL," +
                                              $"RQF INTEGER," +
                                              $"QF INTEGER," +
                                              $"PRIMARY KEY({attribuut})" +
                                              $");";
                        command.ExecuteNonQuery();
                        tables.Add(attribuut);
                    }

                    // For integer values, no single quotes are needed
                    command.CommandText = $"INSERT INTO {attribuut} ({attribuut}, Posting, IDF, RQF, QF) VALUES ('{invulling}', '{posting}', '{IDF}', '{RQF}', '{QF}')";


                    try
                    {
                        command.ExecuteNonQuery();
                    }
                    catch (SqliteException ex)
                    {
                        //throw new Exception($"Error inserting data into table {attribuut}: {ex.Message}");
                        Console.WriteLine($"Error inserting data into table {attribuut}: {ex.Message}");
                    }
                }
            }
        }
    }



    public static string NoPoint(string attribuut)
    {
        string noPoint = attribuut.Replace(".", ",");
        return noPoint;
    }

    public static string NoComma(string attribuut)
    {
        string noPoint = attribuut.Replace(",", ".");
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
            default: return value; //rest becomes text
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
            string attribute = NoPoint(queries[i]).Trim(); 
            string value = NoPoint(reader.GetString(i).Replace("\r", "").Replace("\n", "").Replace("'", "").Trim());
            //queries zijn de types die we hebben (oftewel de attributen van een auto)
            keyValues.Add(((attribute, value), id));
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

            //add to table
            if (!tablesforDictionary.ContainsKey(keyValue.Item1.Item2))
            {
                //Dit zijn de tablename die verbonden wordt met de een van de postinlist. Zodat je de table kan opzoeken. 
                tablesforDictionary[keyValue.Item1.Item2] = new List<string>() { keyValue.Item1.Item1 };
            }
            else tablesforDictionary[ keyValue.Item1.Item2].Add(keyValue.Item1.Item1);
        }
    }

    public void CalculateIDF()
    {
        // n is aantal (unieke??) numerieke tuples nog doen!!
        int n = 10;
        // deze moet je berekenen voor elke kolom ( dus een rij erbij?) 
        int h = 10; 

        // attribuut, waarde van attribuut, metadata
        foreach (KeyValuePair<(string,string), MetaData> data in Database)
        {
            if (data.Value.DocumentFrequency != 0)
            {
                if (ReturnType(data.Key.Item1) == "text")
                {
                    data.Value.IDF = Math.Log(N_totaldocuments / data.Value.DocumentFrequency);

                }
                else
                {
                    data.Value.IDF = Math.Log(N_totaldocuments / data.Value.DocumentFrequency);
                    //data.Value.IDF = 0; 
                    /*
                     foreach(float i in column)
                    {
                    
                        // voor elke waarde i in het attribuut die we zegmaar onderzoeken 
                        data.Value.IDF += Math.Exp(-0.5 * Math.Pow((i - data.Value) / h, 2);
                        
                    }
                     
                     */


                }
            }
            else data.Value.IDF = 0;

        }
    }

        //(float) (MetaDatabase[q].RQF + 1) / (RGQFmax() + 1);
    public void CalculateQF()
    {

        foreach (KeyValuePair<(string, string), MetaData> data in Database)
        {
            
            data.Value.QF = (float) (data.Value.RQF + 1) / (RQFMax[AttributeToIndex(data.Key.Item1)] + 1);
        }
    }


    

    Boolean Contains(string s, char c)
    {
        return s.Contains(c);
    }


    public int AttributeToIndex(string s)
    {
        for (int i = 0; i < queries.Length; i++)
        {
            if (queries[i] == s)
            {
                return i - 1;
            }
        }

        return -1;
    }

    
}
