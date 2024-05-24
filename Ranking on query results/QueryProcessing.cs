using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;



//The second program will be able to process queries and show the answers,
//making use of the metadb for ranking.
//Note that your metadb should be constructed only once,
//for this particular contents of the db and before processing a batch of queries.



/*
 *(Her)definities
   Gegeven attribuut Ak
   Voor elke waarde v uit dom(Ak) is Fk(v) het aantal tuples in R met Ak = v
   Laat N = |R| ;
   we definiëren idfk v) = log(N/Fk(v))
 *
 */

 namespace project;

 public class OutputData
 {
     public string Tupel;
     public float Score;

     public OutputData(string Tupel, float Score)
     {
         this.Tupel = Tupel;
         this.Score = Score;

     }


     public static void ReOrder(OutputData[] Output, float score, string[] tupel)
     {
         for (int i = 0; i < Output.Length; i++)
         {
             if (Output[i] == null)
             {
                 Output[i] = new OutputData(string.Join(",", tupel), score);
                 return;
             }
             else if (Output[i].Score < score)
             {
                 OutputData temp = Output[i];
                 Output[i] = new OutputData(string.Join(",", tupel), score);
                 ReOrder(Output, temp.Score, temp.Tupel.Split(","));
                 return;
             }
         }

     }
 }

 public class QueryProccessing
 {
     private Preprocessing preproces;
     private int k = 10;

     private string VoorbeeldQuery = "SELECT * FROM autompg WHERE brand IN ('audi','bmw','mercedes-benz','volkswagen')";//"SELECT * FROM autompg WHERE model_year = '82' AND type = 'sedan'";

    
    private Dictionary<(string, string), MetaData> MetaDatabase;

    public QueryProccessing()
    {
        MetaDatabase = new Dictionary<(string, string), MetaData>();
        this.preproces = new Preprocessing();
        //preproces.Processing();
        GetAllFromMetaDB();


        Console.WriteLine("You can query now:");

        Query(VoorbeeldQuery, k);
        //Query(Console.ReadLine(), k);

    }

    public void Query(string query, int k)
    {
        //Save query for later preprocsessing
        //Save(query);

        OutputData[] Output = new OutputData[k];

        //split Query in terms
        var Qterms = Preprocessing.MapQuery(query);

        string[] QueryTerms = new string[Qterms.Count];
        for (int i = 0; i < Qterms.Count; i++)
        {
            QueryTerms[i] = Qterms[i].Item1.Item2;
        }

        List<float>[] PostingLists = new List<float>[Qterms.Count];
        //Get postinglists of all attributes of queries
        for (int i = 0; i < Qterms.Count; i++)
        {
            PostingLists[i] = MetaDatabase[Qterms[i].Item1].PostingList;
        }

        //Order tuples on attribute similarity


        //Get tuples of that postinglists
        List<string[]> TupelLists = GetDatabaseTupels();

        //calculate similarity between query and tuples
        foreach (var tupel in TupelLists)
        {
            float scoret = Sim(tupel, Qterms);

            //Reorder top k
            OutputData.ReOrder(Output, scoret, tupel);
        }


        //Output top k
        foreach (var output in Output)
        {
            if (output != null)
            {
                Console.WriteLine(output.Tupel);



            }
            else Console.WriteLine("No more results");

            //Thresholdagorithm


            /*
                "SELECT TOP K R.*" +
                "FROM R" +
                "ORDER BY" +
                "((CASE WHEN R.A1 = q1 THEN 1 ELSE 0 END) +(CASE WHEN R.A2 = q2 THEN 1 ELSE 0 END) +...(CASE WHEN R.Am = qm THEN 1 ELSE 0 END))" +
                "DESC";
            */


        }
    }

    public void GetAllFromMetaDB()
    {
        using (var connection = new SqliteConnection(preproces.metadb))
        {
            connection.Open();

            using (var command = connection.CreateCommand())
            {

                command.CommandText = "SELECT name FROM sqlite_master WHERE type='table';";
                using (var readers = command.ExecuteReader())
                {
                    while (readers.Read())
                    {
                        string table = readers.GetString(0);

                        using (var tableCommand = connection.CreateCommand())
                        {
                            tableCommand.CommandText = $"SELECT * FROM {table};";
                            using (var reader = tableCommand.ExecuteReader())
                            {
                                while (reader.Read())
                                {

                                    string attribute = reader.GetString(0);
                                    string posting = reader.GetString(1);
                                    string idf = reader.GetString(2);
                                    string rqf = reader.GetString(3);
                                    string qf = reader.GetString(4);

                                    MetaDatabase.Add((table, attribute), new MetaData(posting, idf, rqf, qf));
                                }
                            }
                        }
                    }
                }
            }
        }
    }


    public void ThresholdAlgorithm()
    {
        

    }


    //redo
    public void Save(string query)
    {
        using (var connection = new SqliteConnection(preproces.metadb))
        {
            connection.Open();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = query;
                command.ExecuteNonQuery();
            }
        }
    }

    public List<string[]> GetDatabaseTupels()
    {
        List<string[]> Tupels = new List<string[]>();

        using (var connection = new SqliteConnection(preproces.database))
        {
            connection.Open();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT * FROM autompg";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string[] tupel = new string[reader.FieldCount];
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            tupel[i] = reader.GetString(i);
                        }
                        Tupels.Add(tupel);
                    }
                }
            }
        }

        return Tupels;
    }
    

    //t is een lijst van termen, q is een lijst van query termen
    public float Sim(string[] t, List<((string,string), int)> q)
    {
        float sum = 0;
        int m = t.Length;

        //sum to m: (S(t,q),
        //start at 1 to skip ID
        for (int i = 1; i < m; i++)
        {
            for (int j = 0; j < q.Count; j++)
            {
                string table = Preprocessing.queries[i];
                 sum += S(table, t[i], q[j].Item1);
            }
        }

        return sum;
    }

    public float QFSimularity(string table, string u, string v)
    {
        if (u != v)
            return 0;

        return QF(table, u);
    }


    public float QF(string table, string q)
    {

        //RQF(V) / RGQFmax
        return MetaDatabase[(table, q)].QF;

    }


    //S(ti,qj) = J(W(t),W(q) * QF(q)), 
    // similarity tussen een query term q en een term t 
    public float S(string table, string t, (string, string) q)
    {
        return J(W(table, t), W(q.Item1, q.Item2)) * QFSimularity(table, t, q.Item2);
    }

    //subset van de queries in de workload waarin waarde v voorkomt in een in clause voor een specifiek attribuut
    public List<float> W(string table, string attribute)
    {
        //Dit is gewoon de postinglist van t
        try
        {

            return MetaDatabase[(table, attribute)].PostingList;
        }
        catch
        {
         //   Console.WriteLine($"Attribute not found {table}, {attribute}");
            return new List<float>();
        }
    }


    //VERY FOUT
    public List<float> Query(string attribute, List<string> SearchTerms)
    {
        List<float> Posting = null;
        using (var connection = new SqliteConnection(preproces.metadb))
        {
            connection.Open();

            using (var command = connection.CreateCommand())
            {
                //Omdat je beide de tabelnaam en de attribuutnaam nodig hebt
                string Table = preproces.LookUpTable(attribute);

                string CommandText = "SELECT"; 
                foreach (string term in SearchTerms)
                {
                    CommandText += $"{term},";
                }

                //Remove omdat het anders breekt
                CommandText = CommandText.Remove(CommandText.Length - 1);

                command.CommandText = $"{CommandText} FROM {Table} WHERE {Table}={attribute} ";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string list = reader.GetString(1);
                        Posting = StringToList(list);

                    }
                }
            }
        }

        return Posting;
    }

    public static List<float> StringToList(string list)
    {
        List<float> result = new List<float>();
        string[] values = list.Trim().Split(' ');

        foreach (string value in values)
        {
            if(value != "")
                result.Add(float.Parse(value));
        }

        return result;
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
}

public static class Program
{
    public static void Main()
    {
        
        QueryProccessing queryProccessing = new QueryProccessing();



    }
}