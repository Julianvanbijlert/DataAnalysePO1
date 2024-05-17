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

 public class QueryProccessing
 {
     private Preprocessing preproces;


    public QueryProccessing()
    {
        this.preproces = new Preprocessing();
        preproces.Processing();

    }



    public float RGQFmax()
    {
        throw (new NotImplementedException());
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
        if (u != v)
            return 0;

        return QF(u);
    }


    public float QF(string q)
    {
        throw (new NotImplementedException("RQF NOT FILLED YET"));
        //RQF(V) / RGQFmax
       // return (float)RQF[q] / RGQFmax();

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
        //TODO: implementatie van W, database query
        //Dit is gewoon de postinglist van t

     throw (new NotImplementedException("W NOT FILLED YET"));
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
                        string list = reader.GetString(0);
                        Posting = StringToList(list);

                    }
                }
            }
        }

        return Posting;
    }

    public List<float> StringToList(string list)
    {
        List<float> result = new List<float>();
        string[] values = list.Split(' ');

        foreach (string value in values)
        {
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