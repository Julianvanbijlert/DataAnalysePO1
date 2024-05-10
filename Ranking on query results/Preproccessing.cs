using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using Microsoft.Data.Sqlite;

//The first program will do some preprocessing on the data and/or the workload.
//During this phase, a meta database will be constructed and filled.
//This metadb will be used when answering the actual queries.

 namespace project;

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
    public Dictionary<string, List<float>> Database = new Dictionary<string, List<float>>();
    public Dictionary<string, List<string>> tablesforDictionary = new Dictionary<string, List<string>>();
    public string[] queries = new string[] { "mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model_year", "origin", "brand", "model", "type" };


    public Dictionary<string, string[]> types = new Dictionary<string, string[]>();
    //"C:/Users/julia/OneDrive/Bureaublad/UniUtrecht/Leerjaar 2 2023-2024/P4 Data Analyse en Retrieval/Opdrachten/Ranking on query results/Dataset.db";
    private readonly string database = "Data Source=Dataset.db";
    private readonly string table = "autompg";
    private readonly string filepathDBfill = "../../../autompg.sql";
    private readonly string metadb = "Data Source=metadb.db";

    public void Processing()
    {
        
        ReFillDatabase(database);

        using (var conn = new SqliteConnection(database))
        {
            conn.Open();
            var command = conn.CreateCommand();




            command.CommandText = $"SELECT * FROM {table}";

            //now we process all the data
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

                    //niet boeinnd dit was gewoon ff om te kijken
                    int id = reader.GetInt32(0);
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
                    
                    List<(string, string, int)> keyValues = Map(id, text);
                    Reduce(keyValues);

                    
                }
            }
        }

        DropDatabase(metadb);
        //fill metadb
        DictionaryToDatabase(Database, tablesforDictionary, metadb);
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
            // Open the connection
            connection.Open();

            // Read SQL script file
            string script = File.ReadAllText(filepathDBfill);

            // Split script into individual commands
            string[] commands = script.Split(';');

            // Execute each command
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


    public void DictionaryToDatabase(Dictionary<string, List<float>> chosenDictionary, Dictionary<string, List<string>> tableDictionary, string databasePath)
    {
       
        using (var connection = new SqliteConnection(databasePath))
        {
            connection.Open();

            using (var command = connection.CreateCommand())
            {
                foreach (KeyValuePair<string, List<string>> entry in tableDictionary)
                {
                    string key = entry.Key;
                    List<string> value = entry.Value;

                    command.CommandText = $"CREATE TABLE {key} (id integer NOT NULL, {key} text, PRIMARY KEY (id));";
                    command.ExecuteNonQuery();
                }


                foreach (KeyValuePair<string, List<float>> entry in chosenDictionary)
                {
                    string key = entry.Key;
                    List<float> value = entry.Value;

                    command.CommandText = $"INSERT INTO {key} VALUES ({value})";
                    command.ExecuteNonQuery();
                }
            }
        }
    }








    public float RGQFmax()
    {
        throw(new NotImplementedException());
    }

    //TODO fill RQF functie

    //t is een lijst van termen, q is een lijst van query termen
    public float Sim(string[] t, string[] q)
    {
        float sum = 0;
        int m = q.Length;

        //sum to m: (S(t,q),
        for (int i = 0; i < m; i++)
        {
            sum += S(t[i], q[i]);
        }

        return sum;
    }

    public float QFSimularity(string u, string v)
    {
        if(u != v)
            return 0;

        return QF(u);
    }


    public float QF(string q)
    {
        throw(new NotImplementedException("RQF NOT FILLED YET"));
        //RQF(V) / RGQFmax
        return (float)RQF[q] / RGQFmax();

    }


    //S(ti,qj) = J(W(t),W(q) * QF(q)), 
    // similarity tussen een query term q en een term t 
    public float S(string t, string q)
    {
        return J(W(t), W(q)) * QFSimularity(q, t);
    }

    //subset van de queries in de workload waarin waarde v voorkomt in een in clause voor een specifiek attribuut
    public List<float> W(string attribute)
    {
        //Dit is gewoon de postinglist van t

        return Database[attribute];
    }


    //| W(t)| and |W(q)| /  | W(t)| or |W(q)|
    //Jaquard coefficient meet de similarity tussen 2 sets
    public float SlowJ(List<float> t, List<float> q)
    {
        List<float> intersection = Intersect(t, q);
        List<float> union = Union(t, q);

        return intersection.Count / union.Count;
    }

    //set intersection
    public List<float> Intersect(List<float> t, List<float> q)
    {
        List<float> result = new List<float>();
        int i1 = 0;
        int i2 = 0;

        while (i1 < t.Count && i2 < q.Count)
        {

            if (t[i1] == q[i2])
            {
                result.Add(t[i1]);
                i1++;
                i2++;
            }
            else if (t[i1] < q[i2])
            {
                i1++;
            }
            else
            {
                i2++;
            }

        }

        return result;
    }
    //set union
    public List<float> Union(List<float> t, List<float> q)
    {
        //stop alle van t in de set 
        List<float> result = new List<float>(t);

        HashSet<float> set = new HashSet<float>(t);


        //en voeg de van q toe als ze er nog niet in zitten
        for (int i = 0; i < q.Count; i++)
        {
            if (!set.Contains(q[i]))
            {
                result.Add(q[i]);
                set.Add(q[i]);
            }
        }

        return result;
    }


    //length set intersection / length set union
    public float J(List<float> t, List<float> q)
    {
        return FastIntersect(t, q) / FastUnion(t, q);
    }
    //length set intersection
    public float FastIntersect(List<float> t, List<float> q)
    {
        int result = 0;
        int i1 = 0;
        int i2 = 0;

        while (i1 < t.Count && i2 < q.Count)
        {

            if (t[i1] == q[i2])
            {
                //update length
               result++;

                //update index
                i1++;
                i2++;
            }
            else if (t[i1] < q[i2])
            {
                i1++;
            }
            else
            {
                i2++;
            }

        }

        return result;
    }
    //length set union
    public float FastUnion(List<float> t, List<float> q)
    {
        //stop alle van t in de set 
        float result = t.Count;
        HashSet<float> set = new HashSet<float>(t);


        //en voeg de van q toe als ze er nog niet in zitten
        for (int i = 0; i < q.Count; i++)
        {
            if (!set.Contains(q[i]))
            {
                result++;
                set.Add(q[i]);
            }
        }

        return result;
    }

    public List<(string, string, int)> Map(int id, string text)
    {

        List<(string, string, int)> keyValues = new List<(string, string, int)>();
        int i = 0;
        foreach (string word in text.Split(" "))
        {
            string s = queries[i];
            keyValues.Add((word, s, id));
        }

        return keyValues;
    }


    public void Reduce(List<(string, string, int)> keyValues)
    {
        foreach ((string, string, int) keyValue in keyValues)
        {
            //add to pointerlist
            if (Database.ContainsKey(keyValue.Item1))
            {
                Database[keyValue.Item1].Add(keyValue.Item3);
            }
            else
            {
                Database[keyValue.Item1] = new List<float>(){ keyValue.Item3 };
            }

            //add tables
            if (tablesforDictionary.ContainsKey(keyValue.Item1))
            {
                tablesforDictionary[keyValue.Item2].Add(keyValue.Item1);
            }
            else
            {
                tablesforDictionary[keyValue.Item2] = new List<string>() { keyValue.Item1 };
            }
        }
    }


    
}
